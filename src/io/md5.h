#pragma once

#include <array>
#include <cstdint>
#include <cstring>
#include <string>
#include <iomanip>
#include <sstream>
#include <span>

#ifdef MINI_HADOOP_USE_COMMON_CRYPTO
#include <CommonCrypto/CommonDigest.h>
#else
#include <openssl/md5.h>
#endif

#include "io/writable.h"

namespace mini_hadoop {

class MD5Hash : public WritableComparable {
 public:
  static constexpr size_t kMD5Len = 16;

  MD5Hash() { digest_.fill(0); }

  explicit MD5Hash(std::span<const uint8_t> data) {
    std::memcpy(digest_.data(), data.data(), kMD5Len);
  }

  static MD5Hash Compute(std::span<const uint8_t> data) {
    MD5Hash h;
#ifdef MINI_HADOOP_USE_COMMON_CRYPTO
    CC_MD5(data.data(), static_cast<CC_LONG>(data.size()), h.digest_.data());
#else
    ::MD5(data.data(), data.size(), h.digest_.data());
#endif
    return h;
  }

  static MD5Hash Compute(std::string_view s) {
    return Compute(std::span(reinterpret_cast<const uint8_t*>(s.data()), s.size()));
  }

  std::array<uint8_t, kMD5Len> GetDigest() const { return digest_; }

  void Write(OutputBuffer& out) const override {
    out.WriteRawBytes(digest_.data(), kMD5Len);
  }

  void ReadFields(InputBuffer& in) override {
    in.ReadRawBytes(digest_.data(), kMD5Len);
  }

  int CompareTo(const WritableComparable& other) const override {
    auto& o = static_cast<const MD5Hash&>(other);
    return std::memcmp(digest_.data(), o.digest_.data(), kMD5Len);
  }

  std::string ToHexString() const {
    std::ostringstream ss;
    for (auto b : digest_) ss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(b);
    return ss.str();
  }

  bool operator==(const MD5Hash& o) const {
    return digest_ == o.digest_;
  }

 private:
  std::array<uint8_t, kMD5Len> digest_;
};

}  // namespace mini_hadoop
