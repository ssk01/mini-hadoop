#pragma once

#include <atomic>
#include <chrono>
#include <thread>
#include <spdlog/spdlog.h>

#include "io/buffer.h"
#include "ipc/client.h"
#include "ndfs/block_storage.h"
#include "ndfs/data_xceive_server.h"
#include "ndfs/name_node.h"

namespace mini_hadoop {
namespace ndfs {

class DataNode {
 public:
  DataNode(const std::string& data_dir, const std::string& namenode_host,
           int namenode_port, int xceiver_port)
      : storage_(data_dir), namenode_host_(namenode_host),
        namenode_port_(namenode_port),
        xceiver_server_(storage_, xceiver_port) {
    info_.host = "127.0.0.1";
    info_.port = xceiver_port;
    info_.capacity = storage_.GetCapacity();
    info_.remaining = storage_.GetRemaining();
    info_.name = info_.host + ":" + std::to_string(xceiver_port);
  }

  bool Start() {
    if (!xceiver_server_.Start()) return false;
    if (!nn_client_.Connect(namenode_host_, namenode_port_)) return false;

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
    xceiver_server_.Stop();
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
  DataXceiveServer xceiver_server_;
  std::string namenode_host_;
  int namenode_port_;
  std::atomic<bool> running_{false};
  std::thread heartbeat_thread_;
};

}  // namespace ndfs
}  // namespace mini_hadoop
