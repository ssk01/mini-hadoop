#pragma once

#include <atomic>
#include <cstdint>
#include <cstring>
#include <functional>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <spdlog/spdlog.h>

#include "common/error.h"
#include "ndfs/block.h"
#include "ndfs/block_storage.h"

namespace mini_hadoop {
namespace ndfs {

static constexpr uint8_t kOpWriteBlock = 80;
static constexpr uint8_t kOpReadBlock = 81;
static constexpr uint8_t kOpChecksumOk = 82;

class DataXceiveServer {
 public:
  DataXceiveServer(BlockStorage& storage, int port)
      : storage_(storage), port_(port) {}

  bool Start() {
    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) return false;

    int opt = 1;
    setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(static_cast<uint16_t>(port_));
    if (bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0
        || listen(listen_fd_, 128) < 0) {
      close(listen_fd_); return false;
    }

    running_ = true;
    accept_thread_ = std::thread(&DataXceiveServer::AcceptLoop, this);
    for (int i = 0; i < 4; i++)
      workers_.emplace_back(&DataXceiveServer::WorkerLoop, this);
    spdlog::info("DataXceiveServer started on port {}", port_);
    return true;
  }

  void Stop() {
    running_ = false;
    if (listen_fd_ >= 0) { shutdown(listen_fd_, SHUT_RDWR); close(listen_fd_); listen_fd_ = -1; }
    if (accept_thread_.joinable()) accept_thread_.join();
    for (auto& t : workers_) if (t.joinable()) t.join();
  }

  int Port() const { return port_; }

 private:
  int ReadFull(int fd, void* buf, size_t len) {
    size_t total = 0;
    while (total < len) {
      auto n = recv(fd, static_cast<char*>(buf) + total, len - total, 0);
      if (n <= 0) return -1;
      total += static_cast<size_t>(n);
    }
    return 0;
  }

  int WriteFull(int fd, const void* buf, size_t len) {
    size_t total = 0;
    while (total < len) {
      auto n = send(fd, static_cast<const char*>(buf) + total, len - total, 0);
      if (n <= 0) return -1;
      total += static_cast<size_t>(n);
    }
    return 0;
  }

  void AcceptLoop() {
    while (running_) {
      sockaddr_in ca{}; socklen_t al = sizeof(ca);
      int fd = accept(listen_fd_, reinterpret_cast<sockaddr*>(&ca), &al);
      if (fd < 0) continue;
      std::lock_guard<std::mutex> lk(queue_mutex_);
      pending_fds_.push_back(fd);
      queue_cv_.notify_one();
    }
  }

  void WorkerLoop() {
    while (running_) {
      int fd = -1;
      {
        std::unique_lock<std::mutex> lk(queue_mutex_);
        queue_cv_.wait(lk, [this] { return !pending_fds_.empty() || !running_; });
        if (!running_) return;
        if (!pending_fds_.empty()) { fd = pending_fds_.back(); pending_fds_.pop_back(); }
      }
      if (fd >= 0) HandleConnection(fd);
    }
  }

  void HandleConnection(int fd) {
    uint8_t op;
    if (ReadFull(fd, &op, 1) < 0) { close(fd); return; }

    if (op == kOpWriteBlock) {
      int64_t block_id;
      int64_t block_len;
      if (ReadFull(fd, &block_id, 8) < 0 || ReadFull(fd, &block_len, 8) < 0) { close(fd); return; }
      block_id = static_cast<int64_t>(NetToHost64(static_cast<uint64_t>(block_id)));
      block_len = static_cast<int64_t>(NetToHost64(static_cast<uint64_t>(block_len)));

      std::vector<uint8_t> data(static_cast<size_t>(block_len));
      if (ReadFull(fd, data.data(), data.size()) < 0) { close(fd); return; }

      Block b{block_id, block_len, 0};
      storage_.StoreBlock(b, data.data(), static_cast<size_t>(block_len));
      uint8_t ack = 1; WriteFull(fd, &ack, 1);
    } else if (op == kOpReadBlock) {
      int64_t block_id;
      if (ReadFull(fd, &block_id, 8) < 0) { close(fd); return; }
      block_id = static_cast<int64_t>(NetToHost64(static_cast<uint64_t>(block_id)));

      std::vector<uint8_t> data;
      auto st = storage_.ReadBlock(block_id, data);
      if (!st.ok()) { close(fd); return; }

      int64_t len_be = HostToNet64(static_cast<uint64_t>(data.size()));
      WriteFull(fd, &len_be, 8);
      WriteFull(fd, data.data(), data.size());
    }

    close(fd);
  }

  BlockStorage& storage_;
  int port_;
  int listen_fd_ = -1;
  std::atomic<bool> running_{false};
  std::thread accept_thread_;
  std::vector<std::thread> workers_;
  std::mutex queue_mutex_;
  std::condition_variable queue_cv_;
  std::vector<int> pending_fds_;

  static uint64_t HostToNet64(uint64_t v) {
    return (static_cast<uint64_t>(htonl(v & 0xFFFFFFFF)) << 32) | htonl(static_cast<uint32_t>(v >> 32));
  }
  static uint64_t NetToHost64(uint64_t v) {
    return (static_cast<uint64_t>(ntohl(v & 0xFFFFFFFF)) << 32) | ntohl(static_cast<uint32_t>(v >> 32));
  }
};

}  // namespace ndfs
}  // namespace mini_hadoop
