#pragma once

#include <functional>
#include <string>
#include <vector>
#include <memory>
#include "io/writable_types.h"

namespace mini_hadoop {
namespace mapred {

template <typename K, typename V>
class OutputCollector {
 public:
  virtual ~OutputCollector() = default;
  virtual void Collect(const K& key, const V& value) = 0;
};

template <typename K1, typename V1, typename K2, typename V2>
class Mapper {
 public:
  virtual ~Mapper() = default;
  virtual void Map(const K1& key, const V1& value,
                   OutputCollector<K2, V2>& output) = 0;
};

template <typename K2, typename V2, typename K3, typename V3>
class Reducer {
 public:
  virtual ~Reducer() = default;
  virtual void Reduce(const K2& key, const std::vector<V2>& values,
                      OutputCollector<K3, V3>& output) = 0;
};

template <typename K>
class Partitioner {
 public:
  virtual ~Partitioner() = default;
  virtual int GetPartition(const K& key, int num_partitions) = 0;
};

template <typename K>
class HashPartitioner : public Partitioner<K> {
 public:
  int GetPartition(const K& key, int num_partitions) override {
    return (std::hash<std::string>{}(key.ToString()) & 0x7FFFFFFF) % num_partitions;
  }
};

template <typename K, typename V>
class Combiner {
 public:
  virtual ~Combiner() = default;
  virtual void Combine(const K& key, const std::vector<V>& values,
                       OutputCollector<K, V>& output) = 0;
};

}  // namespace mapred
}  // namespace mini_hadoop

#include <map>
#include <vector>
#include <algorithm>

namespace mini_hadoop {
namespace mapred {

template <typename K, typename V>
struct CombiningCollector : public OutputCollector<K, V> {
  std::map<std::string, int64_t> buffer;
  std::unique_ptr<Combiner<K, V>>* combiner;
  int flush_threshold = 10000;

  void Collect(const K& key, const V& value) override {
    buffer[key.ToString()] += value.Get();
    if (static_cast<int>(buffer.size()) >= flush_threshold && combiner && *combiner) {
      Flush();
    }
  }

  void Flush() {
    if (!combiner || !*combiner || buffer.empty()) return;
    class Sink : public OutputCollector<K, V> {
     public:
      std::vector<std::pair<std::string, int64_t>>* out;
      void Collect(const K& k, const V& v) override {
        out->push_back({k.ToString(), v.Get()});
      }
    };
    std::vector<std::pair<std::string, int64_t>> combined;
    Sink s; s.out = &combined;

    for (const auto& [k, v] : buffer) {
      std::vector<V> vals = {V(v)};
      (*combiner)->Combine(K(k), vals, s);
    }
    buffer.clear();
    for (const auto& [k, v] : combined) buffer[k] = v;
  }
};

}  // namespace mapred
}  // namespace mini_hadoop
