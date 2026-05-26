#pragma once

#include <cstdint>
#include <random>
#include <string>

namespace mini_hadoop {
namespace ndfs {

struct Block {
  int64_t block_id = 0;
  int64_t num_bytes = 0;
  int64_t generation_stamp = 0;

  static Block Generate(int64_t num_bytes) {
    static std::random_device rd;
    static std::mt19937_64 gen(rd());
    Block b;
    b.block_id = static_cast<int64_t>(gen());
    b.num_bytes = num_bytes;
    b.generation_stamp = 0;
    return b;
  }
};

}  // namespace ndfs
}  // namespace mini_hadoop
