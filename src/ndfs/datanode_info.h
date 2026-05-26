#pragma once

#include <cstdint>
#include <string>

namespace mini_hadoop {
namespace ndfs {

struct DataNodeInfo {
  std::string name;
  std::string host;
  int port = 0;
  int64_t capacity = 0;
  int64_t remaining = 0;
  int64_t last_update = 0;
};

}  // namespace ndfs
}  // namespace mini_hadoop
