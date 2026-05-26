#pragma once

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>
#include <stdexcept>
#include "io/writable.h"

namespace mini_hadoop {

class IntWritable : public WritableComparable {
 public:
  IntWritable() = default;
  explicit IntWritable(int32_t v) : value_(v) {}

  int32_t Get() const { return value_; }
  void Set(int32_t v) { value_ = v; }

  void Write(OutputBuffer& out) const override { out.WriteInt(value_); }
  void ReadFields(InputBuffer& in) override { value_ = in.ReadInt(); }

  int CompareTo(const WritableComparable& other) const override {
    auto& o = static_cast<const IntWritable&>(other);
    return value_ - o.value_;
  }
  std::string ToString() const { return std::to_string(value_); }

 private:
  int32_t value_ = 0;
};

class LongWritable : public WritableComparable {
 public:
  LongWritable() = default;
  explicit LongWritable(int64_t v) : value_(v) {}

  int64_t Get() const { return value_; }
  void Set(int64_t v) { value_ = v; }

  void Write(OutputBuffer& out) const override { out.WriteLong(value_); }
  void ReadFields(InputBuffer& in) override { value_ = in.ReadLong(); }

  int CompareTo(const WritableComparable& other) const override {
    auto& o = static_cast<const LongWritable&>(other);
    return (value_ < o.value_) ? -1 : (value_ == o.value_ ? 0 : 1);
  }
  std::string ToString() const { return std::to_string(value_); }

 private:
  int64_t value_ = 0;
};

class FloatWritable : public WritableComparable {
 public:
  FloatWritable() = default;
  explicit FloatWritable(float v) : value_(v) {}

  float Get() const { return value_; }
  void Set(float v) { value_ = v; }

  void Write(OutputBuffer& out) const override { out.WriteFloat(value_); }
  void ReadFields(InputBuffer& in) override { value_ = in.ReadFloat(); }

  int CompareTo(const WritableComparable& other) const override {
    auto& o = static_cast<const FloatWritable&>(other);
    return (value_ < o.value_) ? -1 : (value_ == o.value_ ? 0 : 1);
  }

 private:
  float value_ = 0.0f;
};

class BooleanWritable : public WritableComparable {
 public:
  BooleanWritable() = default;
  explicit BooleanWritable(bool v) : value_(v) {}

  bool Get() const { return value_; }

  void Write(OutputBuffer& out) const override { out.WriteBool(value_); }
  void ReadFields(InputBuffer& in) override { value_ = in.ReadBool(); }

  int CompareTo(const WritableComparable& other) const override {
    auto& o = static_cast<const BooleanWritable&>(other);
    return static_cast<int>(value_) - static_cast<int>(o.value_);
  }

 private:
  bool value_ = false;
};

class UTF8 : public WritableComparable {
 public:
  UTF8() = default;
  explicit UTF8(std::string s) : value_(std::move(s)) {}

  const std::string& Get() const { return value_; }
  void Set(std::string s) { value_ = std::move(s); }
  size_t Length() const { return value_.size(); }

  void Write(OutputBuffer& out) const override {
    auto len = static_cast<uint16_t>(value_.size());
    out.WriteShort(static_cast<int16_t>(len));
    out.WriteRawBytes(value_.data(), value_.size());
  }

  void ReadFields(InputBuffer& in) override {
    auto len = in.ReadUnsignedShort();
    value_.resize(len);
    in.ReadRawBytes(value_.data(), len);
  }

  int CompareTo(const WritableComparable& other) const override {
    auto& o = static_cast<const UTF8&>(other);
    return value_.compare(o.value_);
  }

  std::string ToString() const { return value_; }

 private:
  std::string value_;
};

class BytesWritable : public Writable {
 public:
  BytesWritable() = default;
  explicit BytesWritable(std::vector<uint8_t> data) : value_(std::move(data)) {}

  const std::vector<uint8_t>& Get() const { return value_; }

  void Write(OutputBuffer& out) const override { out.WriteBytes(value_); }
  void ReadFields(InputBuffer& in) override { value_ = in.ReadBytes(); }

  bool operator==(const BytesWritable& other) const {
    return value_ == other.value_;
  }

 private:
  std::vector<uint8_t> value_;
};

class NullWritable : public WritableComparable {
 public:
  void Write(OutputBuffer&) const override {}
  void ReadFields(InputBuffer&) override {}

  int CompareTo(const WritableComparable&) const override { return 0; }
};

}  // namespace mini_hadoop
