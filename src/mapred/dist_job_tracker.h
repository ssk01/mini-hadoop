#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include <spdlog/spdlog.h>

#include "io/buffer.h"
#include "io/writable_types.h"
#include "ipc/server.h"
#include "mapred/api/mapper.h"
#include "mapred/api/input_format.h"

namespace mini_hadoop {
namespace mapred {

using MapperFactory = std::function<std::unique_ptr<Mapper<LongWritable, UTF8, UTF8, LongWritable>>()>;
using ReducerFactory = std::function<std::unique_ptr<Reducer<UTF8, LongWritable, UTF8, LongWritable>>()>;

struct MapOutputLocation {
  std::string task_id;
  std::string host;
  int port;
  int partition;
};

enum class TaskType { kMap, kReduce };

struct Task {
  std::string task_id;
  std::string job_id;
  TaskType type;
  int partition;
  std::string input_path;
  std::string output_path;
  std::vector<MapOutputLocation> map_outputs;
  bool is_speculative = false;
};

enum class JobState { kPending, kRunning, kSucceeded, kFailed };

class DistJobTracker {
 public:
  DistJobTracker(int port,
                 MapperFactory mapper_factory,
                 ReducerFactory reducer_factory)
      : server_(port),
        mapper_factory_(std::move(mapper_factory)),
        reducer_factory_(std::move(reducer_factory)) {
    SetupHandlers();
  }

  ~DistJobTracker() { server_.Stop(); }

  bool Start() {
    if (!server_.Start()) return false;
    spec_running_ = true;
    spec_thread_ = std::thread(&DistJobTracker::SpecLoop, this);
    spdlog::info("JobTracker started on port {}", server_.Port());
    return true;
  }

  void Stop() {
    spec_running_ = false;
    server_.Stop();
    if (spec_thread_.joinable()) spec_thread_.join();
  }

  int SpeculativeLaunched() const { return spec_launched_.load(); }

  bool IsJobDone() const { return job_done_.load(); }

 private:
  void SetupHandlers() {
    // op 0: SubmitJob
    server_.RegisterHandler(0, [this](auto& req, auto& resp) {
      std::string input_path = req.ReadString();
      std::string output_path = req.ReadString();
      int32_t num_maps = req.ReadInt();
      int32_t num_reduces = req.ReadInt();

      job_id_ = "job_001";
      input_path_ = input_path;
      output_path_ = output_path;
      num_maps_ = num_maps;
      num_reduces_ = num_reduces;
      maps_completed_ = 0;
      reduces_completed_ = 0;
      job_state_ = JobState::kRunning;

      // Create map tasks
      for (int i = 0; i < num_maps_; i++) {
        Task t;
        t.task_id = job_id_ + "_m_" + std::to_string(i);
        t.job_id = job_id_;
        t.type = TaskType::kMap;
        t.partition = i;
        t.input_path = input_path;
        pending_maps_.push_back(t);
      }

      spdlog::info("JobTracker: job submitted, {} maps, {} reduces", num_maps_, num_reduces_);
      resp.WriteInt(1);  // OK
    });

    // op 1: PollForTask
    server_.RegisterHandler(1, [this](auto& req, auto& resp) {
      std::string tracker_host = req.ReadString();
      int tracker_port = req.ReadInt();

      std::lock_guard<std::mutex> lock(mutex_);

      // Assign pending map tasks first
      if (!pending_maps_.empty()) {
        Task t = pending_maps_.back();
        pending_maps_.pop_back();

        resp.WriteInt(1);  // has task
        resp.WriteString(t.task_id);
        resp.WriteInt(static_cast<int>(t.type));
        resp.WriteInt(t.partition);
        if (t.type == TaskType::kMap) {
          resp.WriteString(t.input_path);
          resp.WriteString("");  // no output path for map
          resp.WriteString("");  // no map output locations
        } else {
          resp.WriteString("");
          resp.WriteString(output_path_);
          resp.WriteInt(0);
        }
        return;
      }

      // All maps done? Create and assign reduce tasks
      if (maps_completed_ >= num_maps_ && pending_reduces_created_ < num_reduces_) {
        int r = pending_reduces_created_++;

        Task t;
        t.task_id = job_id_ + "_r_" + std::to_string(r);
        t.job_id = job_id_;
        t.type = TaskType::kReduce;
        t.partition = r;
        t.output_path = output_path_;
        t.map_outputs = map_output_locations_;

        pending_reduces_.push_back(t);
      }

      if (!pending_reduces_.empty()) {
        Task t = pending_reduces_.back();
        pending_reduces_.pop_back();

        resp.WriteInt(1);  // has task
        resp.WriteString(t.task_id);
        resp.WriteInt(static_cast<int>(t.type));
        resp.WriteInt(t.partition);
        resp.WriteString("");
        resp.WriteString(t.output_path);
        // Serialize map output locations
        resp.WriteInt(static_cast<int32_t>(t.map_outputs.size()));
        for (const auto& loc : t.map_outputs) {
          resp.WriteString(loc.task_id);
          resp.WriteString(loc.host);
          resp.WriteInt(loc.port);
          resp.WriteInt(loc.partition);
        }
        return;
      }

      // No tasks available
      resp.WriteInt(0);
      if (maps_completed_ >= num_maps_ && reduces_completed_ >= num_reduces_) {
        job_state_ = JobState::kSucceeded;
        job_done_ = true;
      }
    });

    // op 2: TaskCompleted
    server_.RegisterHandler(2, [this](auto& req, auto& resp) {
      std::string task_id = req.ReadString();
      int task_type = req.ReadInt();

      {
        std::lock_guard<std::mutex> lock(mutex_);
        if (task_type == static_cast<int>(TaskType::kMap)) {
          int32_t num_parts = req.ReadInt();
          std::string host = req.ReadString();
          int port = req.ReadInt();
          for (int i = 0; i < num_parts; i++) {
            MapOutputLocation loc;
            loc.task_id = task_id;
            loc.host = host;
            loc.port = port;
            loc.partition = i;
            map_output_locations_.push_back(loc);
          }
          maps_completed_++;
          spdlog::info("Map {} completed ({}/{})", task_id, maps_completed_.load(), num_maps_);
        } else {
          reduces_completed_++;
          spdlog::info("Reduce {} completed ({}/{})", task_id, reduces_completed_.load(), num_reduces_);
        }
      }

      resp.WriteInt(1);
    });
  }

  ipc::RpcServer server_;
  MapperFactory mapper_factory_;
  ReducerFactory reducer_factory_;

  std::mutex mutex_;
  std::string job_id_;
  std::string input_path_;
  std::string output_path_;
  int num_maps_ = 0;
  int num_reduces_ = 0;
  std::atomic<int> maps_completed_{0};
  std::atomic<int> reduces_completed_{0};
  int pending_reduces_created_ = 0;
  std::vector<Task> pending_maps_;
  std::vector<Task> pending_reduces_;
  std::vector<MapOutputLocation> map_output_locations_;
  std::atomic<JobState> job_state_{JobState::kPending};
  std::atomic<bool> job_done_{false};
};

}  // namespace mapred
}  // namespace mini_hadoop
