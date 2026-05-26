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

}  // namespace mapred
}  // namespace mini_hadoop
