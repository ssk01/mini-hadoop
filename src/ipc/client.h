#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <spdlog/spdlog.h>

#include "common/error.h"
#include "io/buffer.h"
#include "io/writable.h"

namespace mini_hadoop {
namespace ipc {

class RpcClient {
 public:
  RpcClient() = default;

  ~RpcClient() { Disconnect(); }

  bool Connect(const std::string& host, int port) {
    fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (fd_ < 0) return false;

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));
    inet_pton(AF_INET, host.c_str(), &addr.sin_addr);

    if (connect(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
      close(fd_);
      fd_ = -1;
      return false;
    }

    spdlog::debug("RpcClient: connected to {}:{}", host, port);
    return true;
  }

  void Disconnect() {
    if (fd_ >= 0) {
      close(fd_);
      fd_ = -1;
    }
  }

  bool IsConnected() const { return fd_ >= 0; }

  Status Call(int method_id, const Writable& request, Writable& response) {
    OutputBuffer req_buf;
    req_buf.WriteInt(method_id);
    request.Write(req_buf);

    OutputBuffer frame;
    frame.WriteInt(static_cast<int32_t>(req_buf.Size()));
    frame.WriteRawBytes(req_buf.Data().data(), req_buf.Size());

    if (WriteFull(frame.Data().data(), frame.Size()) < 0)
      return Status::IOError("send failed");

    uint32_t raw_len;
    if (ReadFull(&raw_len, 4) < 0)
      return Status::IOError("recv failed");

    uint32_t resp_len = ntohl(raw_len);
    std::vector<uint8_t> resp_data(resp_len);
    if (ReadFull(resp_data.data(), resp_len) < 0)
      return Status::IOError("recv failed");

    InputBuffer resp_buf;
    resp_buf.Reset(resp_data);
    response.ReadFields(resp_buf);
    return Status::OK();
  }

  Status CallRaw(int method_id, std::span<const uint8_t> req_data,
                 std::vector<uint8_t>& resp_data) {
    OutputBuffer frame;
    frame.WriteInt(method_id);
    frame.WriteRawBytes(req_data.data(), req_data.size());

    OutputBuffer outer;
    outer.WriteInt(static_cast<int32_t>(frame.Size()));
    outer.WriteRawBytes(frame.Data().data(), frame.Size());

    if (WriteFull(outer.Data().data(), outer.Size()) < 0)
      return Status::IOError("send failed");

    uint32_t raw_len;
    if (ReadFull(&raw_len, 4) < 0)
      return Status::IOError("recv failed");

    uint32_t resp_len = ntohl(raw_len);
    resp_data.resize(resp_len);
    if (ReadFull(resp_data.data(), resp_len) < 0)
      return Status::IOError("recv failed");

    return Status::OK();
  }

 private:
  int ReadFull(void* buf, size_t len) {
    size_t total = 0;
    while (total < len) {
      ssize_t n = recv(fd_, static_cast<char*>(buf) + total, len - total, 0);
      if (n <= 0) return -1;
      total += static_cast<size_t>(n);
    }
    return 0;
  }

  int WriteFull(const void* buf, size_t len) {
    size_t total = 0;
    while (total < len) {
      ssize_t n = send(fd_, static_cast<const char*>(buf) + total, len - total, 0);
      if (n <= 0) return -1;
      total += static_cast<size_t>(n);
    }
    return 0;
  }

  int fd_ = -1;
};

}  // namespace ipc
}  // namespace mini_hadoop
