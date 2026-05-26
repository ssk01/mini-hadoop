#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <spdlog/spdlog.h>

#include "io/buffer.h"
#include "ipc/client.h"
#include "ndfs/block.h"
#include "ndfs/datanode_info.h"
#include "ndfs/name_node.h"

namespace mini_hadoop {
namespace ndfs {

class DfsClient {
 public:
  DfsClient(const std::string& namenode_host, int namenode_port,
            int64_t block_size = kDefaultBlockSize)
      : nn_host_(namenode_host), nn_port_(namenode_port),
        block_size_(block_size) {}

  Status Connect() {
    if (!nn_client_.Connect(nn_host_, nn_port_))
      return Status::IOError("cannot connect to NameNode");
    return Status::OK();
  }

  void Disconnect() { nn_client_.Disconnect(); }

  Status Mkdirs(const std::string& path) {
    OutputBuffer req;
    req.WriteString(path);

    std::vector<uint8_t> resp;
    auto st = nn_client_.CallRaw(static_cast<int>(NdfsOp::kMkdirs), req.Data(), resp);
    if (!st.ok()) return st;

    InputBuffer in(resp);
    if (!in.ReadBool()) return Status::IOError("mkdirs failed: " + path);
    return Status::OK();
  }

  Status Delete(const std::string& path) {
    OutputBuffer req;
    req.WriteString(path);

    std::vector<uint8_t> resp;
    auto st = nn_client_.CallRaw(static_cast<int>(NdfsOp::kDelete), req.Data(), resp);
    if (!st.ok()) return st;

    InputBuffer in(resp);
    if (!in.ReadBool()) return Status::IOError("delete failed: " + path);
    return Status::OK();
  }

  std::vector<std::string> List(const std::string& path) {
    OutputBuffer req;
    req.WriteString(path);

    std::vector<uint8_t> resp;
    auto st = nn_client_.CallRaw(static_cast<int>(NdfsOp::kGetListing), req.Data(), resp);
    if (!st.ok()) return {};

    InputBuffer in(resp);
    int32_t count = in.ReadInt();
    std::vector<std::string> result;
    for (int i = 0; i < count; i++) result.push_back(in.ReadString());
    return result;
  }

  Status WriteFile(const std::string& path, const uint8_t* data, size_t total_len, bool overwrite = true) {
    if (total_len == 0) return Status::OK();

    size_t offset = 0;
    std::vector<int64_t> written_blocks;
    bool first_block = true;

    while (offset < total_len) {
      size_t chunk_size = std::min(static_cast<size_t>(block_size_), total_len - offset);

      // Get block allocation from NameNode
      int op = first_block ? static_cast<int>(NdfsOp::kStartFile) : static_cast<int>(NdfsOp::kAddBlock);
      OutputBuffer req;
      req.WriteString(path);
      if (first_block) req.WriteBool(overwrite);

      std::vector<uint8_t> resp;
      auto st = nn_client_.CallRaw(op, req.Data(), resp);
      if (!st.ok()) return st;

      InputBuffer in(resp);
      Block block = ReadBlock(in);
      auto targets = ReadDataNodeInfoList(in);

      // Write block via pipeline: send to first DN with downstream targets
      if (!targets.empty()) {
        auto& first_dn = targets[0];
        ipc::RpcClient dn_client;
        if (dn_client.Connect(first_dn.host, first_dn.port)) {
          OutputBuffer block_req;
          block_req.WriteLong(block.block_id);
          block_req.WriteLong(static_cast<int64_t>(chunk_size));
          block_req.WriteInt(static_cast<int32_t>(targets.size()) - 1);  // remaining targets
          for (size_t i = 1; i < targets.size(); i++) {
            block_req.WriteString(targets[i].host);
            block_req.WriteInt(targets[i].port);
            block_req.WriteLong(targets[i].capacity);
            block_req.WriteLong(targets[i].remaining);
            block_req.WriteLong(targets[i].last_update);
          }
          block_req.WriteRawBytes(data + offset, chunk_size);

          std::vector<uint8_t> block_resp;
          dn_client.CallRaw(0, block_req.Data(), block_resp);
          dn_client.Disconnect();
        }
      }

      // Confirm to NameNode
      for (const auto& dn : targets) {
        OutputBuffer confirm;
        confirm.WriteString(dn.name);
        confirm.WriteLong(block.block_id);
        confirm.WriteLong(static_cast<int64_t>(chunk_size));
        confirm.WriteLong(block.generation_stamp);
        confirm.WriteString("");

        std::vector<uint8_t> cresp;
        nn_client_.CallRaw(static_cast<int>(NdfsOp::kBlockReceived), confirm.Data(), cresp);
      }

      written_blocks.push_back(block.block_id);
      offset += chunk_size;
      first_block = false;
    }

    spdlog::info("write: {} ({} bytes, {} blocks)", path, total_len, written_blocks.size());
    return Status::OK();
  }

  Status ReadFile(const std::string& path, std::vector<uint8_t>& data) {
    OutputBuffer req;
    req.WriteString(path);

    std::vector<uint8_t> resp;
    auto st = nn_client_.CallRaw(static_cast<int>(NdfsOp::kGetBlocks), req.Data(), resp);
    if (!st.ok()) return st;

    InputBuffer in(resp);
    int32_t block_count = in.ReadInt();
    data.clear();

    for (int i = 0; i < block_count; i++) {
      Block block = ReadBlock(in);
      auto targets = ReadDataNodeInfoList(in);

      bool read_ok = false;
      for (const auto& dn : targets) {
        ipc::RpcClient dn_client;
        if (!dn_client.Connect(dn.host, dn.port)) continue;

        OutputBuffer block_req;
        block_req.WriteLong(block.block_id);

        std::vector<uint8_t> block_resp;
        if (dn_client.CallRaw(1, block_req.Data(), block_resp).ok()) {  // op 1 = read block
          InputBuffer bin(block_resp);
          auto chunk = bin.ReadBytes();
          data.insert(data.end(), chunk.begin(), chunk.end());
          read_ok = true;
          dn_client.Disconnect();
          break;
        }
        dn_client.Disconnect();
      }

      if (!read_ok) return Status::IOError("cannot read block " + std::to_string(block.block_id));
    }

    spdlog::info("read: {} ({} bytes)", path, data.size());
    return Status::OK();
  }

 private:
  static Block ReadBlock(InputBuffer& in) {
    Block b;
    b.block_id = in.ReadLong();
    b.num_bytes = in.ReadLong();
    b.generation_stamp = in.ReadLong();
    return b;
  }

  static DataNodeInfo ReadDataNodeInfo(InputBuffer& in) {
    DataNodeInfo dn;
    dn.name = in.ReadString();
    dn.host = in.ReadString();
    dn.port = in.ReadInt();
    dn.capacity = in.ReadLong();
    dn.remaining = in.ReadLong();
    dn.last_update = in.ReadLong();
    return dn;
  }

  static std::vector<DataNodeInfo> ReadDataNodeInfoList(InputBuffer& in) {
    int32_t count = in.ReadInt();
    std::vector<DataNodeInfo> result;
    for (int i = 0; i < count; i++) result.push_back(ReadDataNodeInfo(in));
    return result;
  }

  ipc::RpcClient nn_client_;
  std::string nn_host_;
  int nn_port_;
  int64_t block_size_;
};

}  // namespace ndfs
}  // namespace mini_hadoop
