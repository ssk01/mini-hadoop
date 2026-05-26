#pragma once

#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "common/error.h"
#include "ndfs/block.h"

namespace mini_hadoop {
namespace ndfs {

class BlockStorage {
 public:
  explicit BlockStorage(const std::string& data_dir) : data_dir_(data_dir) {
    std::filesystem::create_directories(data_dir);
  }

  Status StoreBlock(const Block& block, const uint8_t* data, size_t len) {
    auto path = BlockPath(block.block_id);
    std::ofstream f(path, std::ios::binary);
    if (!f) return Status::IOError("cannot create block file: " + path);
    f.write(reinterpret_cast<const char*>(data), static_cast<std::streamsize>(len));
    f.close();
    return Status::OK();
  }

  Status ReadBlock(int64_t block_id, std::vector<uint8_t>& data) {
    auto path = BlockPath(block_id);
    std::ifstream f(path, std::ios::binary | std::ios::ate);
    if (!f) return Status::IOError("cannot open block: " + path);
    auto size = f.tellg();
    f.seekg(0);
    data.resize(static_cast<size_t>(size));
    f.read(reinterpret_cast<char*>(data.data()), size);
    f.close();
    return Status::OK();
  }

  Status RemoveBlock(int64_t block_id) {
    auto path = BlockPath(block_id);
    if (std::remove(path.c_str()) != 0)
      return Status::IOError("cannot remove block: " + path);
    return Status::OK();
  }

  int64_t GetCapacity() const {
    std::error_code ec;
    auto space = std::filesystem::space(data_dir_, ec);
    if (ec) return 0;
    return static_cast<int64_t>(space.capacity);
  }

  int64_t GetRemaining() const {
    std::error_code ec;
    auto space = std::filesystem::space(data_dir_, ec);
    if (ec) return 0;
    return static_cast<int64_t>(space.available);
  }

  std::vector<Block> GetBlockReport() const {
    std::vector<Block> blocks;
    for (const auto& entry : std::filesystem::directory_iterator(data_dir_)) {
      if (!entry.is_regular_file()) continue;
      if (entry.path().extension() != ".blk") continue;

      auto stem = entry.path().stem().string();
      int64_t bid = std::stoll(stem);
      Block b;
      b.block_id = bid;
      b.num_bytes = entry.file_size();
      blocks.push_back(b);
    }
    return blocks;
  }

 private:
  std::string BlockPath(int64_t block_id) const {
    return data_dir_ + "/" + std::to_string(block_id) + ".blk";
  }

  std::string data_dir_;
};

}  // namespace ndfs
}  // namespace mini_hadoop
