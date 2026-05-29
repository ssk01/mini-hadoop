#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <spdlog/spdlog.h>

#include "io/buffer.h"
#include "ipc/client.h"
#include "ndfs/block.h"
#include "ndfs/datanode_info.h"
#include "ndfs/data_xceive_server.h"
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

      // Write block via DataXceiveServer (op 80 = write)
      if (!targets.empty()) {
        int sock = socket(AF_INET, SOCK_STREAM, 0);
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(static_cast<uint16_t>(targets[0].port));
        inet_pton(AF_INET, targets[0].host.c_str(), &addr.sin_addr);

        if (connect(sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0) {
          uint8_t op = kOpWriteBlock;
          send(sock, &op, 1, 0);

          int64_t bid_be = HostToNet64(static_cast<uint64_t>(block.block_id));
          int64_t len_be = HostToNet64(static_cast<uint64_t>(chunk_size));
          send(sock, &bid_be, 8, 0);
          send(sock, &len_be, 8, 0);
          send_full(sock, data + offset, chunk_size);

          uint8_t ack;
          read(sock, &ack, 1);
          close(sock);
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

      // Read block via DataXceiveServer
      bool read_ok = false;
      for (const auto& dn : targets) {
        int sock = socket(AF_INET, SOCK_STREAM, 0);
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(static_cast<uint16_t>(dn.port));
        inet_pton(AF_INET, dn.host.c_str(), &addr.sin_addr);

        if (connect(sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) continue;

        uint8_t op = kOpReadBlock;
        send(sock, &op, 1, 0);
        int64_t bid_be = HostToNet64(static_cast<uint64_t>(block.block_id));
        send(sock, &bid_be, 8, 0);

        int64_t data_len_be;
        if (recv_full(sock, &data_len_be, 8) < 0) { close(sock); continue; }
        int64_t data_len = static_cast<int64_t>(NetToHost64(static_cast<uint64_t>(data_len_be)));

        auto prev_size = data.size();
        data.resize(prev_size + static_cast<size_t>(data_len));
        if (recv_full(sock, data.data() + prev_size, static_cast<size_t>(data_len)) < 0) {
          data.resize(prev_size); close(sock); continue;
        }
        close(sock);
        read_ok = true;
        break;
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

  static int send_full(int fd, const void* buf, size_t len) {
    size_t total = 0;
    while (total < len) {
      auto n = send(fd, static_cast<const char*>(buf) + total, len - total, 0);
      if (n <= 0) return -1;
      total += static_cast<size_t>(n);
    }
    return 0;
  }

  static int recv_full(int fd, void* buf, size_t len) {
    size_t total = 0;
    while (total < len) {
      auto n = recv(fd, static_cast<char*>(buf) + total, len - total, 0);
      if (n <= 0) return -1;
      total += static_cast<size_t>(n);
    }
    return 0;
  }

  static uint64_t HostToNet64(uint64_t v) {
    return (static_cast<uint64_t>(htonl(v & 0xFFFFFFFF)) << 32)
         | htonl(static_cast<uint32_t>(v >> 32));
  }

  static uint64_t NetToHost64(uint64_t v) {
    return (static_cast<uint64_t>(ntohl(v & 0xFFFFFFFF)) << 32)
         | ntohl(static_cast<uint32_t>(v >> 32));
  }
};

}  // namespace ndfs
}  // namespace mini_hadoop
