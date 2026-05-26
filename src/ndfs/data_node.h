#pragma once

#include <atomic>
#include <chrono>
#include <thread>
#include <spdlog/spdlog.h>

#include "io/buffer.h"
#include "ipc/client.h"
#include "ipc/server.h"
#include "ndfs/block_storage.h"
#include "ndfs/datanode_info.h"
#include "ndfs/fs_namesystem.h"

namespace mini_hadoop {
namespace ndfs {

class DataNode {
 public:
  DataNode(const std::string& data_dir, const std::string& namenode_host,
           int namenode_port, int block_server_port)
      : storage_(data_dir), namenode_host_(namenode_host),
        namenode_port_(namenode_port), block_server_(block_server_port) {
    info_.host = "127.0.0.1";
    info_.port = block_server_port;
    info_.capacity = storage_.GetCapacity();
    info_.remaining = storage_.GetRemaining();
    info_.name = info_.host + ":" + std::to_string(block_server_port);

    block_server_.RegisterHandler(0, [this](auto& req, auto& resp) {
      int64_t block_id = req.ReadLong();
      int64_t data_len = req.ReadLong();
      int32_t num_targets = req.ReadInt();

      // Read list of downstream DataNodes
      struct Target { std::string host; int port; };
      std::vector<Target> downstream;
      for (int i = 0; i < num_targets; i++) {
        Target t;
        t.host = req.ReadString();
        t.port = req.ReadInt();
        // skip capacity/remaining/lastUpdate
        req.ReadLong(); req.ReadLong(); req.ReadLong();
        downstream.push_back(t);
      }

      // Read block data
      std::vector<uint8_t> data(static_cast<size_t>(data_len));
      req.ReadRawBytes(data.data(), static_cast<size_t>(data_len));
      Block b{block_id, data_len, 0};
      auto st = storage_.StoreBlock(b, data.data(), static_cast<size_t>(data_len));

      // Forward to next DN in pipeline
      if (!downstream.empty() && st.ok()) {
        auto& next = downstream[0];
        ipc::RpcClient pipe_client;
        if (pipe_client.Connect(next.host, next.port)) {
          OutputBuffer fwd;
          fwd.WriteLong(block_id);
          fwd.WriteLong(data_len);
          fwd.WriteInt(num_targets - 1);
          for (size_t i = 1; i < downstream.size(); i++) {
            fwd.WriteString(downstream[i].host);
            fwd.WriteInt(downstream[i].port);
            fwd.WriteLong(0); fwd.WriteLong(0); fwd.WriteLong(0);
          }
          fwd.WriteRawBytes(data.data(), static_cast<size_t>(data_len));
          std::vector<uint8_t> dummy;
          pipe_client.CallRaw(0, fwd.Data(), dummy);
          pipe_client.Disconnect();
        }
      }

      resp.WriteInt(st.ok() ? 1 : 0);
    });

    block_server_.RegisterHandler(1, [this](auto& req, auto& resp) {
      int64_t block_id = req.ReadLong();
      std::vector<uint8_t> data;
      auto st = storage_.ReadBlock(block_id, data);
      if (st.ok()) {
        resp.WriteBytes(data);
      } else {
        resp.WriteInt(-1);
      }
    });
  }

  bool Start() {
    if (!block_server_.Start()) {
      spdlog::error("DataNode: block server failed");
      return false;
    }
    if (!nn_client_.Connect(namenode_host_, namenode_port_)) {
      spdlog::error("DataNode: cannot connect to NameNode {}:{}", namenode_host_, namenode_port_);
      return false;
    }

    SendHeartbeat();
    SendBlockReport();

    running_ = true;
    heartbeat_thread_ = std::thread(&DataNode::HeartbeatLoop, this);
    spdlog::info("DataNode {} started", info_.name);
    return true;
  }

  void Stop() {
    running_ = false;
    if (heartbeat_thread_.joinable()) heartbeat_thread_.join();
    nn_client_.Disconnect();
    block_server_.Stop();
  }

  BlockStorage& Storage() { return storage_; }

 private:
  void SendHeartbeat() {
    OutputBuffer req;
    req.WriteString(info_.name);
    req.WriteString(info_.host);
    req.WriteInt(info_.port);
    req.WriteLong(storage_.GetCapacity());
    req.WriteLong(storage_.GetRemaining());
    std::vector<uint8_t> resp;
    nn_client_.CallRaw(static_cast<int>(NdfsOp::kHeartbeat), req.Data(), resp);
  }

  void SendBlockReport() {
    auto blocks = storage_.GetBlockReport();
    OutputBuffer req;
    req.WriteString(info_.name);
    req.WriteInt(static_cast<int32_t>(blocks.size()));
    for (const auto& b : blocks) {
      req.WriteLong(b.block_id);
      req.WriteLong(b.num_bytes);
      req.WriteLong(b.generation_stamp);
    }
    std::vector<uint8_t> resp;
    nn_client_.CallRaw(static_cast<int>(NdfsOp::kBlockReport), req.Data(), resp);
  }

  void HeartbeatLoop() {
    while (running_) {
      SendHeartbeat();
      std::this_thread::sleep_for(std::chrono::seconds(3));
    }
  }

  BlockStorage storage_;
  DataNodeInfo info_;
  ipc::RpcClient nn_client_;
  ipc::RpcServer block_server_;
  std::string namenode_host_;
  int namenode_port_;
  std::atomic<bool> running_{false};
  std::thread heartbeat_thread_;
};

}  // namespace ndfs
}  // namespace mini_hadoop

