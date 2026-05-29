#pragma once

#include <cstdint>
#include <cstring>
#include <fstream>
#include <string>
#include <vector>
#include <random>
#include "common/error.h"
#include "io/buffer.h"
#include "io/md5.h"
#include "io/writable.h"

namespace mini_hadoop {

class SequenceFileWriter {
 public:
  static constexpr uint8_t kMagic[4] = {'S', 'E', 'Q', 2};
  static constexpr int32_t kSyncEscape = -1;
  static constexpr int kSyncInterval = 100;

  SequenceFileWriter() {
    std::mt19937 gen(std::random_device{}());
    for (auto& b : sync_) b = static_cast<uint8_t>(gen());
  }

  Status Open(const std::string& path) {
    file_.open(path, std::ios::binary | std::ios::trunc);
    if (!file_) return Status::IOError("cannot open: " + path);
    WriteRaw(kMagic, 4);
    WriteRaw(sync_.data(), sync_.size());
    count_ = 0;
    return Status::OK();
  }

  Status Append(const Writable& key, const Writable& value) {
    OutputBuffer kb, vb;
    key.Write(kb); value.Write(vb);
    return AppendRaw(kb.Data(), vb.Data());
  }

  Status AppendRaw(std::span<const uint8_t> key_data, std::span<const uint8_t> val_data) {
    if (key_data.empty()) return Status::InvalidArg("zero-length key");

    OutputBuffer record;

    if (count_ > 0 && (count_ % kSyncInterval) == 0) {
      record.WriteInt(kSyncEscape);
      record.WriteRawBytes(sync_.data(), sync_.size());
    }

    record.WriteInt(static_cast<int32_t>(key_data.size() + val_data.size()));
    record.WriteInt(static_cast<int32_t>(key_data.size()));
    record.WriteRawBytes(key_data.data(), key_data.size());
    record.WriteRawBytes(val_data.data(), val_data.size());

    WriteRaw(record.Data().data(), record.Size());
    count_++;
    return Status::OK();
  }

  void Close() { file_.close(); }
  uint64_t Count() const { return count_; }

 private:
  void WriteRaw(const void* p, size_t n) {
    file_.write(static_cast<const char*>(p), static_cast<std::streamsize>(n));
  }

  std::ofstream file_;
  std::array<uint8_t, MD5Hash::kMD5Len> sync_{};
  uint64_t count_ = 0;
};

class SequenceFileReader {
 public:
  Status Open(const std::string& path) {
    file_.open(path, std::ios::binary);
    if (!file_) return Status::IOError("cannot open: " + path);

    uint8_t magic[4];
    if (!ReadRaw(magic, 4)) return Status::Corrupt("not a SequenceFile");
    if (std::memcmp(magic, SequenceFileWriter::kMagic, 4) != 0)
      return Status::Corrupt("not a SequenceFile");
    if (!ReadRaw(sync_.data(), sync_.size()))
      return Status::Corrupt("truncated header");
    return Status::OK();
  }

  bool Next() {
    while (true) {
      int32_t len;
      if (!ReadBE(len)) return false;

      if (len == SequenceFileWriter::kSyncEscape) {
        file_.seekg(MD5Hash::kMD5Len, std::ios::cur);
        continue;
      }

      int32_t key_len;
      if (!ReadBE(key_len)) return false;

      key_buf_.resize(static_cast<size_t>(key_len));
      if (!ReadRaw(key_buf_.data(), key_buf_.size())) return false;

      int32_t val_len = len - key_len;
      val_buf_.resize(static_cast<size_t>(val_len));
      if (!ReadRaw(val_buf_.data(), val_buf_.size())) return false;

      return true;
    }
  }

  std::span<const uint8_t> Key() const { return key_buf_; }
  std::span<const uint8_t> Value() const { return val_buf_; }
  void Close() { file_.close(); }

 private:
  bool ReadRaw(void* buf, size_t n) {
    file_.read(static_cast<char*>(buf), static_cast<std::streamsize>(n));
    return file_.gcount() == static_cast<std::streamsize>(n);
  }

  bool ReadBE(int32_t& v) {
    uint32_t n;
    if (!ReadRaw(&n, 4)) return false;
    v = static_cast<int32_t>(ntohl(n));
    return true;
  }

  std::ifstream file_;
  std::array<uint8_t, MD5Hash::kMD5Len> sync_{};
  std::vector<uint8_t> key_buf_;
  std::vector<uint8_t> val_buf_;
};

}  // namespace mini_hadoop
