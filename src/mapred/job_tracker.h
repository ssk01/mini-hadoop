#pragma once

#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>
#include <algorithm>
#include <spdlog/spdlog.h>

#include "io/buffer.h"
#include "io/writable_types.h"
#include "mapred/api/mapper.h"
#include "mapred/api/input_format.h"

namespace mini_hadoop {
namespace mapred {

class JobConfig {
 public:
  std::string input_path;
  std::string output_path;
  std::string mapper_class;
  std::string reducer_class;
  int num_map_tasks = 1;
  int num_reduce_tasks = 1;
};

using MapperFactory = std::function<std::unique_ptr<Mapper<LongWritable, UTF8, UTF8, LongWritable>>()>;
using ReducerFactory = std::function<std::unique_ptr<Reducer<UTF8, LongWritable, UTF8, LongWritable>>()>;
using CombinerFactory = std::function<std::unique_ptr<Combiner<UTF8, LongWritable>>()>;

struct MapOutput {
  UTF8 key;
  LongWritable value;
};

class JobTracker {
 public:
  JobTracker() = default;

  bool SubmitJob(const JobConfig& config,
                 MapperFactory mapper_factory,
                 ReducerFactory reducer_factory,
                 CombinerFactory combiner_factory = nullptr) {
    config_ = config;
    mapper_factory_ = std::move(mapper_factory);
    reducer_factory_ = std::move(reducer_factory);
    combiner_factory_ = std::move(combiner_factory);
    job_done_ = false;
    return true;
  }

  bool RunJob() {
    spdlog::info("JobTracker: starting job, input={}, output={}",
                 config_.input_path, config_.output_path);

    // --- Map Phase ---
    TextInputFormat input_format;
    auto reader = input_format.CreateReader(config_.input_path);
    auto mapper = mapper_factory_();

    std::vector<std::vector<MapOutput>> partitioned(
        static_cast<size_t>(config_.num_reduce_tasks));

    class MapCollector : public OutputCollector<UTF8, LongWritable> {
     public:
      std::vector<std::vector<MapOutput>>* parts;
      int num_reduces;
      HashPartitioner<UTF8> partitioner;

      void Collect(const UTF8& key, const LongWritable& value) override {
        int p = partitioner.GetPartition(key, num_reduces);
        (*parts)[p].push_back({key, value});
      }
    };

    MapCollector collector;
    collector.parts = &partitioned;
    collector.num_reduces = config_.num_reduce_tasks;

    LongWritable lk;
    UTF8 lv;
    while (reader->Next(lk, lv)) {
      mapper->Map(lk, lv, collector);
    }
    reader->Close();

    spdlog::info("Map phase done, {} partitions", config_.num_reduce_tasks);

    // Sort each partition by key
    for (auto& part : partitioned) {
      std::sort(part.begin(), part.end(), [](const auto& a, const auto& b) {
        return a.key.CompareTo(b.key) < 0;
      });
    }

    // --- Combiner (optional) ---
    if (combiner_factory_) {
      auto combiner = combiner_factory_();
      for (auto& part : partitioned) {
        std::vector<MapOutput> combined;
        size_t j = 0;
        while (j < part.size()) {
          UTF8 key = part[j].key;
          int64_t sum = 0;
          while (j < part.size() && part[j].key.CompareTo(key) == 0) {
            sum += part[j].value.Get();
            j++;
          }
          combined.push_back({key, LongWritable(sum)});
        }
        part = std::move(combined);
      }
    }

    // --- Reduce Phase ---
    auto reducer = reducer_factory_();

    class ReduceCollect : public OutputCollector<UTF8, LongWritable> {
     public:
      std::vector<std::pair<std::string, int64_t>> results;

      void Collect(const UTF8& key, const LongWritable& value) override {
        results.push_back({key.Get(), value.Get()});
      }
    };

    std::vector<std::pair<std::string, int64_t>> all_results;

    for (int i = 0; i < config_.num_reduce_tasks; i++) {
      ReduceCollect rc;

      // Group by key
      size_t j = 0;
      while (j < partitioned[i].size()) {
        UTF8 key = partitioned[i][j].key;
        std::vector<LongWritable> values;
        while (j < partitioned[i].size() &&
               partitioned[i][j].key.CompareTo(key) == 0) {
          values.push_back(partitioned[i][j].value);
          j++;
        }
        reducer->Reduce(key, values, rc);
      }

      all_results.insert(all_results.end(), rc.results.begin(), rc.results.end());
    }

    // Sort output
    std::sort(all_results.begin(), all_results.end());

    // Write output
    std::ofstream out(config_.output_path);
    for (const auto& [k, v] : all_results) {
      out << k << "\t" << v << "\n";
    }
    out.close();

    job_done_ = true;
    spdlog::info("Job done, {} output records", all_results.size());
    return true;
  }

  bool IsJobDone() const { return job_done_; }

 private:
  JobConfig config_;
  MapperFactory mapper_factory_;
  ReducerFactory reducer_factory_;
  CombinerFactory combiner_factory_;
  bool job_done_ = false;
};

}  // namespace mapred
}  // namespace mini_hadoop
