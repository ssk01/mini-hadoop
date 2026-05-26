#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <spdlog/spdlog.h>

#include "io/buffer.h"

namespace mini_hadoop {
namespace ipc {

using RpcHandler = std::function<void(InputBuffer& request, OutputBuffer& response)>;

class RpcServer {
 public:
  RpcServer(int port, int num_handlers = 4)
      : port_(port), num_handlers_(num_handlers) {}

  ~RpcServer() { Stop(); }

  void RegisterHandler(int method_id, RpcHandler handler) {
    handlers_[method_id] = std::move(handler);
  }

  bool Start() {
    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) {
      spdlog::error("RpcServer: socket failed");
      return false;
    }

    int opt = 1;
    setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(static_cast<uint16_t>(port_));

    if (bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
      spdlog::error("RpcServer: bind failed on port {}", port_);
      close(listen_fd_);
      return false;
    }

    if (listen(listen_fd_, 128) < 0) {
      spdlog::error("RpcServer: listen failed");
      close(listen_fd_);
      return false;
    }

    running_ = true;
    acceptor_ = std::thread(&RpcServer::AcceptLoop, this);
    for (int i = 0; i < num_handlers_; i++) {
      workers_.emplace_back(&RpcServer::WorkerLoop, this);
    }

    spdlog::info("RpcServer started on port {}", port_);
    return true;
  }

  void Stop() {
    running_ = false;
    queue_cv_.notify_all();
    if (listen_fd_ >= 0) {
      shutdown(listen_fd_, SHUT_RDWR);
      close(listen_fd_);
      listen_fd_ = -1;
    }
    if (acceptor_.joinable()) acceptor_.join();
    for (auto& t : workers_) {
      if (t.joinable()) t.join();
    }
  }

  int Port() const { return port_; }

 private:
  int ReadFull(int fd, void* buf, size_t len) {
    size_t total = 0;
    while (total < len) {
      ssize_t n = recv(fd, static_cast<char*>(buf) + total, len - total, 0);
      if (n <= 0) return -1;
      total += static_cast<size_t>(n);
    }
    return 0;
  }

  int WriteFull(int fd, const void* buf, size_t len) {
    size_t total = 0;
    while (total < len) {
      ssize_t n = send(fd, static_cast<const char*>(buf) + total, len - total, 0);
      if (n <= 0) return -1;
      total += static_cast<size_t>(n);
    }
    return 0;
  }

  void AcceptLoop() {
    while (running_) {
      sockaddr_in client_addr{};
      socklen_t addr_len = sizeof(client_addr);
      int client_fd = accept(listen_fd_, reinterpret_cast<sockaddr*>(&client_addr), &addr_len);
      if (client_fd < 0) continue;

      {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        pending_fds_.push_back(client_fd);
      }
      queue_cv_.notify_one();
    }
  }

  void WorkerLoop() {
    while (running_) {
      int fd = -1;
      {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        queue_cv_.wait(lock, [this] { return !pending_fds_.empty() || !running_; });
        if (!running_) return;
        if (!pending_fds_.empty()) {
          fd = pending_fds_.back();
          pending_fds_.pop_back();
        }
      }

      if (fd >= 0) HandleConnection(fd);
    }
  }

  void HandleConnection(int fd) {
    while (running_) {
      uint32_t raw_len;
      if (ReadFull(fd, &raw_len, 4) < 0) break;
      uint32_t total_len = ntohl(raw_len);
      if (total_len < 4 || total_len > 64 * 1024 * 1024) break;

      std::vector<uint8_t> msg(total_len);
      if (ReadFull(fd, msg.data(), total_len) < 0) break;

      InputBuffer req;
      req.Reset(msg.data(), msg.size());
      int32_t method_id = req.ReadInt();

      OutputBuffer resp;
      auto it = handlers_.find(method_id);
      if (it != handlers_.end()) {
        it->second(req, resp);
      } else {
        resp.WriteInt(-1);
        resp.WriteString("unknown method");
      }

      OutputBuffer frame;
      frame.WriteInt(static_cast<int32_t>(resp.Size()));
      frame.WriteRawBytes(resp.Data().data(), resp.Size());

      if (WriteFull(fd, frame.Data().data(), frame.Size()) < 0) break;
    }
    close(fd);
  }

  int port_;
  int num_handlers_;
  int listen_fd_ = -1;
  std::atomic<bool> running_{false};

  std::thread acceptor_;
  std::vector<std::thread> workers_;

  std::mutex queue_mutex_;
  std::condition_variable queue_cv_;
  std::vector<int> pending_fds_;

  std::unordered_map<int, RpcHandler> handlers_;
};

}  // namespace ipc
}  // namespace mini_hadoop
