#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <fstream>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>
#include <spdlog/spdlog.h>

#include "io/buffer.h"
#include "io/writable_types.h"
#include "ipc/client.h"
#include "ipc/server.h"
#include "mapred/api/mapper.h"
#include "mapred/api/input_format.h"
#include "mapred/dist_job_tracker.h"

namespace mini_hadoop {
namespace mapred {

class TaskTracker {
 public:
  TaskTracker(const std::string& jt_host, int jt_port,
              const std::string& host, int shuffle_port,
              MapperFactory mapper_factory,
              ReducerFactory reducer_factory)
      : jt_host_(jt_host), jt_port_(jt_port),
        host_(host), shuffle_port_(shuffle_port),
        mapper_factory_(std::move(mapper_factory)),
        reducer_factory_(std::move(reducer_factory)),
        shuffle_server_(shuffle_port) {
    SetupShuffleServer();
  }

  ~TaskTracker() {
    if (running_) Stop();
  }

  bool Start() {
    if (!shuffle_server_.Start()) return false;
    if (!jt_client_.Connect(jt_host_, jt_port_)) return false;

    running_ = true;
    poll_thread_ = std::thread(&TaskTracker::PollLoop, this);
    spdlog::info("TaskTracker {}:{} started", host_, shuffle_port_);
    return true;
  }

  void Stop() {
    running_ = false;
    if (poll_thread_.joinable()) poll_thread_.join();
    jt_client_.Disconnect();
    shuffle_server_.Stop();
  }

 private:
  void SetupShuffleServer() {
    shuffle_server_.RegisterHandler(0, [this](auto& req, auto& resp) {
      std::string task_id = req.ReadString();
      int partition = req.ReadInt();

      std::lock_guard<std::mutex> lock(map_outputs_mutex_);
      auto it = map_outputs_.find({task_id, partition});
      if (it != map_outputs_.end()) {
        resp.WriteInt(1);
        // Write key-value pairs
        resp.WriteInt(static_cast<int32_t>(it->second.size()));
        for (const auto& [k, v] : it->second) {
          resp.WriteString(k);
          resp.WriteLong(v);
        }
      } else {
        resp.WriteInt(0);
      }
    });
  }

  void PollLoop() {
    while (running_) {
      // Poll for task
      OutputBuffer req;
      req.WriteString(host_);
      req.WriteInt(shuffle_port_);

      std::vector<uint8_t> resp;
      if (!jt_client_.CallRaw(1, req.Data(), resp).ok()) {
        std::this_thread::sleep_for(std::chrono::seconds(2));
        continue;
      }

      InputBuffer in(resp);
      int has_task = in.ReadInt();
      if (!has_task) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        continue;
      }

      Task task;
      task.task_id = in.ReadString();
      task.type = static_cast<TaskType>(in.ReadInt());
      task.partition = in.ReadInt();

      if (task.type == TaskType::kMap) {
        task.input_path = in.ReadString();
        in.ReadString();  // skip output_path
        in.ReadInt();  // skip num outputs

        // Execute map task in a thread
        auto tt = task;
        std::thread([this, tt]() {
          ExecuteMapTask(tt);
          ReportTaskComplete(tt);
        }).detach();
      } else {
        in.ReadString();  // skip input_path
        task.output_path = in.ReadString();  // output path for reduce

        int32_t num_locs = in.ReadInt();
        for (int i = 0; i < num_locs; i++) {
          MapOutputLocation loc;
          loc.task_id = in.ReadString();
          loc.host = in.ReadString();
          loc.port = in.ReadInt();
          loc.partition = in.ReadInt();
          task.map_outputs.push_back(loc);
        }

        std::thread([this, tt = task]() {
          ExecuteReduceTask(tt);
          ReportTaskComplete(tt);
        }).detach();
      }
    }
  }

  void ExecuteMapTask(const Task& task) {
    auto reader = std::make_unique<TextRecordReader>(task.input_path);
    auto mapper = mapper_factory_();

    struct MapCollect : OutputCollector<UTF8, LongWritable> {
      std::vector<std::pair<std::string, int64_t>> outputs;
      void Collect(const UTF8& key, const LongWritable& value) override {
        outputs.push_back({key.Get(), value.Get()});
      }
    };

    MapCollect collector;
    LongWritable lk;
    UTF8 lv;
    while (reader->Next(lk, lv)) {
      mapper->Map(lk, lv, collector);
    }
    reader->Close();

    // Sort and partition
    std::sort(collector.outputs.begin(), collector.outputs.end());
    int num_reduces = 1;  // Will get from config; TODO

    {
      std::lock_guard<std::mutex> lock(map_outputs_mutex_);
      for (const auto& [k, v] : collector.outputs) {
        int p = 0;  // Hash partition
        if (!k.empty()) p = static_cast<int>(std::hash<std::string>{}(k) & 0x7FFFFFFF) % num_reduces;
        map_outputs_[{task.task_id, p}].push_back({k, v});
      }
    }

    spdlog::info("MapTask {} done, {} records", task.task_id, collector.outputs.size());
  }

  void ExecuteReduceTask(const Task& task) {
    // Shuffle: fetch map outputs
    std::vector<std::pair<std::string, int64_t>> all_data;

    for (const auto& loc : task.map_outputs) {
      if (loc.partition != task.partition) continue;

      ipc::RpcClient client;
      if (!client.Connect(loc.host, loc.port)) continue;

      OutputBuffer req;
      req.WriteString(loc.task_id);
      req.WriteInt(loc.partition);

      std::vector<uint8_t> resp_data;
      if (!client.CallRaw(0, req.Data(), resp_data).ok()) {
        client.Disconnect();
        continue;
      }

      InputBuffer in(resp_data);
      if (!in.ReadInt()) { client.Disconnect(); continue; }

      int32_t count = in.ReadInt();
      for (int i = 0; i < count; i++) {
        std::string k = in.ReadString();
        int64_t v = in.ReadLong();
        all_data.push_back({k, v});
      }
      client.Disconnect();
    }

    // Sort
    std::sort(all_data.begin(), all_data.end());

    // Reduce
    auto reducer = reducer_factory_();
    std::vector<std::pair<std::string, int64_t>> results;

    size_t i = 0;
    while (i < all_data.size()) {
      std::string key = all_data[i].first;
      std::vector<LongWritable> values;
      while (i < all_data.size() && all_data[i].first == key) {
        values.push_back(LongWritable(all_data[i].second));
        i++;
      }

      struct ReduceCollect : OutputCollector<UTF8, LongWritable> {
        std::vector<std::pair<std::string, int64_t>>* out;
        void Collect(const UTF8& key, const LongWritable& value) override {
          out->push_back({key.Get(), value.Get()});
        }
      };
      ReduceCollect rc;
      rc.out = &results;
      reducer->Reduce(UTF8(key), values, rc);
    }

    // Write results to file
    std::ofstream out(task.output_path);
    for (const auto& [k, v] : results) out << k << "\t" << v << "\n";
    out.close();

    spdlog::info("ReduceTask {} done, {} records", task.task_id, results.size());
  }

  void ReportTaskComplete(const Task& task) {
    OutputBuffer req;
    req.WriteString(task.task_id);
    req.WriteInt(static_cast<int>(task.type));

    if (task.type == TaskType::kMap) {
      std::lock_guard<std::mutex> lock(map_outputs_mutex_);
      std::set<int> parts;
      for (const auto& [key, val] : map_outputs_) {
        parts.insert(key.second);
      }
      req.WriteInt(static_cast<int32_t>(parts.size()));
      req.WriteString(host_);
      req.WriteInt(shuffle_port_);
    }

    std::vector<uint8_t> resp;
    jt_client_.CallRaw(2, req.Data(), resp);
  }

  ipc::RpcClient jt_client_;
  ipc::RpcServer shuffle_server_;
  std::string jt_host_;
  int jt_port_;
  std::string host_;
  int shuffle_port_;
  MapperFactory mapper_factory_;
  ReducerFactory reducer_factory_;
  std::atomic<bool> running_{false};
  std::thread poll_thread_;

  std::mutex map_outputs_mutex_;
  std::map<std::pair<std::string, int>, std::vector<std::pair<std::string, int64_t>>> map_outputs_;
};

}  // namespace mapred
}  // namespace mini_hadoop
