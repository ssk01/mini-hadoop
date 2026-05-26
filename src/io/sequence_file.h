#pragma once

#include <cstdint>
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
    std::random_device rd;
    std::mt19937 gen(rd());
    for (int i = 0; i < MD5Hash::kMD5Len; i++) sync_[i] = static_cast<uint8_t>(gen());
  }

  Status Open(const std::string& path) {
    file_.open(path, std::ios::binary | std::ios::trunc);
    if (!file_) return Status::IOError("cannot open: " + path);
    WriteSpan(std::span(kMagic, 4));
    WriteSpan(std::span(sync_.data(), sync_.size()));
    count_ = 0;
    return Status::OK();
  }

  Status Append(const Writable& key, const Writable& value) {
    OutputBuffer kbuf, vbuf;
    key.Write(kbuf);
    value.Write(vbuf);
    return AppendRaw(kbuf.Data(), vbuf.Data());
  }

  Status AppendRaw(std::span<const uint8_t> key, std::span<const uint8_t> value) {
    if (key.empty()) return Status::InvalidArg("zero-length key");

    OutputBuffer rec;

    if (count_ > 0 && (count_ % kSyncInterval) == 0) {
      rec.WriteInt(kSyncEscape);
      rec.WriteRawBytes(sync_.data(), sync_.size());
    }

    rec.WriteInt(static_cast<int32_t>(key.size() + value.size()));
    rec.WriteInt(static_cast<int32_t>(key.size()));
    rec.WriteRawBytes(key.data(), key.size());
    rec.WriteRawBytes(value.data(), value.size());

    WriteSpan(rec.Data());
    count_++;
    return Status::OK();
  }

  void Close() { file_.close(); }
  uint64_t Count() const { return count_; }

 private:
  void WriteSpan(std::span<const uint8_t> data) {
    file_.write(reinterpret_cast<const char*>(data.data()), static_cast<std::streamsize>(data.size()));
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
    file_.read(reinterpret_cast<char*>(magic), 4);
    if (std::memcmp(magic, SequenceFileWriter::kMagic, 4) != 0)
      return Status::Corrupt("not a SequenceFile");

    file_.read(reinterpret_cast<char*>(sync_.data()), sync_.size());
    return Status::OK();
  }

  bool Next() {
    while (true) {
      int32_t raw_len;
      file_.read(reinterpret_cast<char*>(&raw_len), 4);
      if (file_.gcount() != 4) return false;

      int32_t len = static_cast<int32_t>(ntohl(static_cast<uint32_t>(raw_len)));

      if (len == SequenceFileWriter::kSyncEscape) {
        file_.seekg(MD5Hash::kMD5Len, std::ios::cur);
        continue;
      }

      int32_t raw_key_len;
      file_.read(reinterpret_cast<char*>(&raw_key_len), 4);
      if (file_.gcount() != 4) return false;

      int32_t key_len = static_cast<int32_t>(ntohl(static_cast<uint32_t>(raw_key_len)));

      key_buf_.resize(key_len);
      file_.read(reinterpret_cast<char*>(key_buf_.data()), key_len);

      int32_t val_len = len - key_len;
      val_buf_.resize(val_len);
      file_.read(reinterpret_cast<char*>(val_buf_.data()), val_len);
      return true;
    }
  }

  std::span<const uint8_t> Key() const { return key_buf_; }
  std::span<const uint8_t> Value() const { return val_buf_; }

  void Close() { file_.close(); }

 private:
  std::ifstream file_;
  std::array<uint8_t, MD5Hash::kMD5Len> sync_{};
  std::vector<uint8_t> key_buf_;
  std::vector<uint8_t> val_buf_;
};

}  // namespace mini_hadoop
