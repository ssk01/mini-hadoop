#pragma once

#include <memory>
#include <spdlog/spdlog.h>

#include "io/buffer.h"
#include "io/writable_types.h"
#include "ipc/server.h"
#include "ndfs/fs_namesystem.h"
#include "ndfs/fsimage.h"

namespace mini_hadoop {
namespace ndfs {

// RPC method IDs (matching Nutch 0.7 NDFS protocol)
enum class NdfsOp : int32_t {
  kStartFile = 0,
  kAddBlock = 1,
  kGetBlocks = 2,
  kDelete = 3,
  kMkdirs = 4,
  kGetListing = 5,
  kHeartbeat = 6,
  kBlockReport = 7,
  kBlockReceived = 8,
  kGetStats = 9,
  kGetDatanodeReport = 10,
};

class NameNode {
 public:
  explicit NameNode(int port, const std::string& fsimage_path = "/tmp/mini-hadoop-nn/fsimage")
      : server_(port), fsimage_path_(fsimage_path) {
    FsImage::Load(namesystem_.GetTree(), fsimage_path_);
    SetupHandlers();
  }

  bool Start() {
    if (!server_.Start()) return false;
    spdlog::info("NameNode started on port {}", server_.Port());
    return true;
  }

  void Stop() {
    FsImage::Save(namesystem_.GetTree(), fsimage_path_);
    server_.Stop();
  }

  FsNamesystem& GetNamesystem() { return namesystem_; }

 private:
  void SetupHandlers() {
    server_.RegisterHandler(static_cast<int>(NdfsOp::kStartFile), [this](auto& req, auto& resp) {
      std::string path = ReadString(req);
      bool overwrite = req.ReadBool();
      auto result = namesystem_.StartFile(path, overwrite);
      WriteBlock(resp, result.block);
      WriteDataNodeInfoList(resp, result.targets);
    });

    server_.RegisterHandler(static_cast<int>(NdfsOp::kAddBlock), [this](auto& req, auto& resp) {
      std::string path = ReadString(req);
      auto result = namesystem_.AddBlock(path);
      WriteBlock(resp, result.block);
      WriteDataNodeInfoList(resp, result.targets);
    });

    server_.RegisterHandler(static_cast<int>(NdfsOp::kGetBlocks), [this](auto& req, auto& resp) {
      std::string path = ReadString(req);
      auto result = namesystem_.GetBlocks(path);
      resp.WriteInt(static_cast<int32_t>(result.blocks.size()));
      for (size_t i = 0; i < result.blocks.size(); i++) {
        WriteBlock(resp, result.blocks[i]);
        WriteDataNodeInfoList(resp, result.locations[i]);
      }
    });

    server_.RegisterHandler(static_cast<int>(NdfsOp::kDelete), [this](auto& req, auto& resp) {
      std::string path = ReadString(req);
      resp.WriteBool(namesystem_.Delete(path));
    });

    server_.RegisterHandler(static_cast<int>(NdfsOp::kMkdirs), [this](auto& req, auto& resp) {
      std::string path = ReadString(req);
      resp.WriteBool(namesystem_.Mkdirs(path));
    });

    server_.RegisterHandler(static_cast<int>(NdfsOp::kGetListing), [this](auto& req, auto& resp) {
      std::string path = ReadString(req);
      auto list = namesystem_.GetListing(path);
      resp.WriteInt(static_cast<int32_t>(list.size()));
      for (const auto& item : list) resp.WriteString(item);
    });

    server_.RegisterHandler(static_cast<int>(NdfsOp::kHeartbeat), [this](auto& req, auto& resp) {
      std::string name = ReadString(req);
      std::string host = ReadString(req);
      int32_t port = req.ReadInt();
      int64_t capacity = req.ReadLong();
      int64_t remaining = req.ReadLong();
      namesystem_.Heartbeat(name, host, port, capacity, remaining);
    });

    server_.RegisterHandler(static_cast<int>(NdfsOp::kBlockReport), [this](auto& req, auto& resp) {
      std::string name = ReadString(req);
      int32_t count = req.ReadInt();
      std::vector<Block> blocks(count);
      for (int i = 0; i < count; i++) {
        blocks[i].block_id = req.ReadLong();
        blocks[i].num_bytes = req.ReadLong();
        blocks[i].generation_stamp = req.ReadLong();
      }
      namesystem_.BlockReport(name, blocks);
      resp.WriteInt(1);  // ACK
    });

    server_.RegisterHandler(static_cast<int>(NdfsOp::kBlockReceived), [this](auto& req, auto& resp) {
      std::string dn_name = ReadString(req);
      Block b;
      b.block_id = req.ReadLong();
      b.num_bytes = req.ReadLong();
      b.generation_stamp = req.ReadLong();
      std::string del_hint = ReadString(req);
      namesystem_.BlockReceived(dn_name, b, del_hint);
      resp.WriteInt(1);
    });

    server_.RegisterHandler(static_cast<int>(NdfsOp::kGetStats), [this](auto& req, auto& resp) {
      resp.WriteLong(namesystem_.TotalCapacity());
      resp.WriteLong(namesystem_.TotalRemaining());
      resp.WriteInt(namesystem_.AliveDataNodes());
    });

    server_.RegisterHandler(static_cast<int>(NdfsOp::kGetDatanodeReport), [this](auto& req, auto& resp) {
      auto report = namesystem_.GetDataNodeReport();
      resp.WriteInt(static_cast<int32_t>(report.size()));
      for (const auto& dn : report) WriteDataNodeInfo(resp, dn);
    });
  }

  static std::string ReadString(InputBuffer& in) { return in.ReadString(); }

  static void WriteBlock(OutputBuffer& out, const Block& b) {
    out.WriteLong(b.block_id);
    out.WriteLong(b.num_bytes);
    out.WriteLong(b.generation_stamp);
  }

  static void WriteDataNodeInfo(OutputBuffer& out, const DataNodeInfo& dn) {
    out.WriteString(dn.name);
    out.WriteString(dn.host);
    out.WriteInt(dn.port);
    out.WriteLong(dn.capacity);
    out.WriteLong(dn.remaining);
    out.WriteLong(dn.last_update);
  }

  static void WriteDataNodeInfoList(OutputBuffer& out, const std::vector<DataNodeInfo>& dns) {
    out.WriteInt(static_cast<int32_t>(dns.size()));
    for (const auto& dn : dns) WriteDataNodeInfo(out, dn);
  }

  ipc::RpcServer server_;
  FsNamesystem namesystem_;
  std::string fsimage_path_;
};

}  // namespace ndfs
}  // namespace mini_hadoop
