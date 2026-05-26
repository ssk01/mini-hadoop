#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "ndfs/block.h"
#include "ndfs/datanode_info.h"
#include "ndfs/inode_tree.h"

namespace mini_hadoop {
namespace ndfs {

static constexpr int64_t kDefaultBlockSize = 64 * 1024 * 1024;  // 64MB
static constexpr int kDefaultReplication = 3;

class FsNamesystem {
 public:
  FsNamesystem() = default;

  // === Client Operations ===

  // Start writing a file - returns the first block + DataNode assignments
  struct StartFileResult {
    Block block;
    std::vector<DataNodeInfo> targets;
  };

  StartFileResult StartFile(const std::string& path, bool overwrite) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto* existing = tree_.GetNode(path);
    if (existing && !overwrite) {
      // error handled by caller
    }
    if (existing && overwrite) {
      tree_.Delete(path);
    }

    tree_.Mkdirs(GetParentDir(path));
    auto* file = tree_.CreateFile(path);
    if (!file) return {};

    Block block = Block::Generate(0);  // size determined on close
    block_to_datanodes_[block.block_id] = ChooseTargets(kDefaultReplication);
    file->blocks.push_back(block.block_id);

    return {block, block_to_datanodes_[block.block_id]};
  }

  struct AddBlockResult {
    Block block;
    std::vector<DataNodeInfo> targets;
  };

  AddBlockResult AddBlock(const std::string& path) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto* file = tree_.GetNode(path);
    if (!file || file->type != INode::kFile) return {};

    Block block = Block::Generate(0);
    block_to_datanodes_[block.block_id] = ChooseTargets(kDefaultReplication);
    file->blocks.push_back(block.block_id);

    return {block, block_to_datanodes_[block.block_id]};
  }

  struct GetBlocksResult {
    std::vector<Block> blocks;
    std::vector<std::vector<DataNodeInfo>> locations;
  };

  GetBlocksResult GetBlocks(const std::string& path) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto* file = tree_.GetNode(path);
    if (!file || file->type != INode::kFile) return {};

    GetBlocksResult result;
    for (auto bid : file->blocks) {
      Block b;
      b.block_id = bid;
      b.num_bytes = 0;  // client will determine actual size
      result.blocks.push_back(b);

      auto it = block_to_datanodes_.find(bid);
      if (it != block_to_datanodes_.end()) {
        result.locations.push_back(it->second);
      } else {
        result.locations.push_back({});
      }
    }
    return result;
  }

  bool Delete(const std::string& path) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto* node = tree_.GetNode(path);
    if (!node) return false;

    if (node->type == INode::kFile) {
      for (auto bid : node->blocks) {
        block_to_datanodes_.erase(bid);
      }
    }
    return tree_.Delete(path);
  }

  bool Mkdirs(const std::string& path) {
    std::lock_guard<std::mutex> lock(mutex_);
    return tree_.Mkdirs(path) != nullptr;
  }

  std::vector<std::string> GetListing(const std::string& path) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto nodes = tree_.ListDir(path);
    std::vector<std::string> result;
    for (auto* n : nodes) {
      result.push_back(n->type == INode::kDirectory ? n->name + "/" : n->name);
    }
    return result;
  }

  // === DataNode Operations ===

  void RegisterDataNode(const DataNodeInfo& info) {
    std::lock_guard<std::mutex> lock(mutex_);
    datanodes_[info.name] = info;
    datanodes_[info.name].last_update = CurrentTime();
  }

  void Heartbeat(const std::string& dn_name, const std::string& host, int port,
                 int64_t capacity, int64_t remaining) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = datanodes_.find(dn_name);
    if (it == datanodes_.end()) {
      DataNodeInfo info;
      info.name = dn_name;
      info.host = host;
      info.port = port;
      info.capacity = capacity;
      info.remaining = remaining;
      info.last_update = CurrentTime();
      datanodes_[dn_name] = info;
    } else {
      it->second.host = host;
      it->second.port = port;
      it->second.capacity = capacity;
      it->second.remaining = remaining;
      it->second.last_update = CurrentTime();
    }
  }

  void BlockReport(const std::string& dn_name, const std::vector<Block>& blocks) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = datanodes_.find(dn_name);
    if (it == datanodes_.end()) return;

    dn_to_blocks_[dn_name] = blocks;

    for (const auto& b : blocks) {
      auto& dns = block_to_datanodes_[b.block_id];
      bool found = false;
      for (auto& dn : dns) {
        if (dn.name == dn_name) { found = true; break; }
      }
      if (!found && dns.size() < static_cast<size_t>(kDefaultReplication)) {
        dns.push_back(it->second);
      }
    }
  }

  void BlockReceived(const std::string& dn_name, const Block& block,
                     const std::string& del_hint) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto& dns = block_to_datanodes_[block.block_id];
    auto it = datanodes_.find(dn_name);
    if (it == datanodes_.end()) return;

    bool found = false;
    for (auto& dn : dns) {
      if (dn.name == dn_name) found = true;
    }
    if (!found) dns.push_back(it->second);
  }

  int64_t TotalCapacity() const {
    int64_t total = 0;
    for (const auto& [name, info] : datanodes_) total += info.capacity;
    return total;
  }

  int64_t TotalRemaining() const {
    int64_t total = 0;
    for (const auto& [name, info] : datanodes_) total += info.remaining;
    return total;
  }

  int AliveDataNodes() const { return static_cast<int>(datanodes_.size()); }

  std::vector<DataNodeInfo> GetDataNodeReport() const {
    std::vector<DataNodeInfo> result;
    for (const auto& [name, info] : datanodes_) result.push_back(info);
    return result;
  }

  InodeTree& GetTree() { return tree_; }

 private:
  std::vector<DataNodeInfo> ChooseTargets(int replication) {
    std::vector<DataNodeInfo> targets;
    for (const auto& [name, info] : datanodes_) {
      targets.push_back(info);
      if (static_cast<int>(targets.size()) >= replication) break;
    }
    return targets;
  }

  static std::string GetParentDir(const std::string& path) {
    auto pos = path.rfind('/');
    if (pos == std::string::npos) return "/";
    if (pos == 0) return "/";
    return path.substr(0, pos);
  }

  static int64_t CurrentTime() {
    return std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
  }

  std::mutex mutex_;
  InodeTree tree_;
  std::unordered_map<std::string, DataNodeInfo> datanodes_;
  std::unordered_map<int64_t, std::vector<DataNodeInfo>> block_to_datanodes_;
  std::unordered_map<std::string, std::vector<Block>> dn_to_blocks_;
};

}  // namespace ndfs
}  // namespace mini_hadoop
