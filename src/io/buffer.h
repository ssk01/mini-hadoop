#pragma once

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>
#include <stdexcept>
#include <span>
#include <arpa/inet.h>

namespace mini_hadoop {

class OutputBuffer {
 public:
  OutputBuffer() { buffer_.reserve(256); }

  void Reset() { buffer_.clear(); }

  void WriteByte(uint8_t v) { buffer_.push_back(v); }
  void WriteRawBytes(const void* data, size_t len) {
    const auto* p = static_cast<const uint8_t*>(data);
    buffer_.insert(buffer_.end(), p, p + len);
  }

  void WriteBool(bool v) { WriteByte(v ? 1 : 0); }

  void WriteShort(int16_t v) {
    auto n = htons(static_cast<uint16_t>(v));
    WriteRawBytes(&n, 2);
  }

  void WriteInt(int32_t v) {
    auto n = htonl(static_cast<uint32_t>(v));
    WriteRawBytes(&n, 4);
  }

  void WriteLong(int64_t v) {
    uint32_t hi = htonl(static_cast<uint32_t>(v >> 32));
    uint32_t lo = htonl(static_cast<uint32_t>(v & 0xFFFFFFFF));
    WriteRawBytes(&hi, 4);
    WriteRawBytes(&lo, 4);
  }

  void WriteFloat(float v) {
    static_assert(sizeof(float) == 4);
    uint32_t bits;
    std::memcpy(&bits, &v, 4);
    WriteInt(static_cast<int32_t>(bits));
  }

  void WriteDouble(double v) {
    static_assert(sizeof(double) == 8);
    uint64_t bits;
    std::memcpy(&bits, &v, 8);
    WriteLong(static_cast<int64_t>(bits));
  }

  void WriteBytes(std::span<const uint8_t> data) {
    WriteInt(static_cast<int32_t>(data.size()));
    WriteRawBytes(data.data(), data.size());
  }

  void WriteString(std::string_view s) {
    auto bytes = std::span(reinterpret_cast<const uint8_t*>(s.data()), s.size());
    WriteInt(static_cast<int32_t>(bytes.size()));
    WriteRawBytes(bytes.data(), bytes.size());
  }

  const std::vector<uint8_t>& Data() const { return buffer_; }
  size_t Size() const { return buffer_.size(); }

 private:
  std::vector<uint8_t> buffer_;
};

class InputBuffer {
 public:
  InputBuffer() = default;

  explicit InputBuffer(std::span<const uint8_t> data) {
    Reset(data);
  }

  void Reset(const uint8_t* data, size_t len) {
    data_ = data;
    size_ = len;
    pos_ = 0;
  }

  void Reset(std::span<const uint8_t> data) {
    Reset(data.data(), data.size());
  }

  size_t Position() const { return pos_; }
  size_t Remaining() const { return size_ - pos_; }

  uint8_t ReadByte() {
    CheckBounds(1);
    return data_[pos_++];
  }

  void ReadRawBytes(void* dst, size_t len) {
    CheckBounds(len);
    std::memcpy(dst, data_ + pos_, len);
    pos_ += len;
  }

  bool ReadBool() { return ReadByte() != 0; }

  int16_t ReadShort() {
    CheckBounds(2);
    uint16_t n;
    std::memcpy(&n, data_ + pos_, 2);
    pos_ += 2;
    return static_cast<int16_t>(ntohs(n));
  }

  int32_t ReadInt() {
    CheckBounds(4);
    uint32_t n;
    std::memcpy(&n, data_ + pos_, 4);
    pos_ += 4;
    return static_cast<int32_t>(ntohl(n));
  }

  int64_t ReadLong() {
    CheckBounds(8);
    uint32_t hi, lo;
    std::memcpy(&hi, data_ + pos_, 4);
    std::memcpy(&lo, data_ + pos_ + 4, 4);
    pos_ += 8;
    return (static_cast<int64_t>(ntohl(hi)) << 32) | ntohl(lo);
  }

  float ReadFloat() {
    int32_t bits = ReadInt();
    float v;
    std::memcpy(&v, &bits, 4);
    return v;
  }

  double ReadDouble() {
    int64_t bits = ReadLong();
    double v;
    std::memcpy(&v, &bits, 8);
    return v;
  }

  std::vector<uint8_t> ReadBytes() {
    int32_t len = ReadInt();
    std::vector<uint8_t> result(len);
    ReadRawBytes(result.data(), len);
    return result;
  }

  std::string ReadString() {
    int32_t len = ReadInt();
    std::string result(len, '\0');
    ReadRawBytes(result.data(), len);
    return result;
  }

  uint16_t ReadUnsignedShort() {
    CheckBounds(2);
    uint16_t n;
    std::memcpy(&n, data_ + pos_, 2);
    pos_ += 2;
    return ntohs(n);
  }

  void Skip(size_t n) {
    CheckBounds(n);
    pos_ += n;
  }

 private:
  void CheckBounds(size_t need) {
    if (pos_ + need > size_)
      throw std::runtime_error("InputBuffer: read past end");
  }

  const uint8_t* data_ = nullptr;
  size_t size_ = 0;
  size_t pos_ = 0;
};

}  // namespace mini_hadoop
