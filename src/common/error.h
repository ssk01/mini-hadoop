#pragma once

#include <string>
#include <string_view>

namespace mini_hadoop {

class Status {
 public:
  Status() = default;

  static Status OK() { return {}; }

  static Status IOError(std::string_view msg) {
    Status s;
    s.code_ = Code::kIOError;
    s.msg_ = msg;
    return s;
  }

  static Status Corrupt(std::string_view msg) {
    Status s;
    s.code_ = Code::kCorrupt;
    s.msg_ = msg;
    return s;
  }

  static Status NotFound(std::string_view msg) {
    Status s;
    s.code_ = Code::kNotFound;
    s.msg_ = msg;
    return s;
  }

  static Status InvalidArg(std::string_view msg) {
    Status s;
    s.code_ = Code::kInvalidArg;
    s.msg_ = msg;
    return s;
  }

  bool ok() const { return code_ == Code::kOk; }
  std::string_view message() const { return msg_; }

 private:
  enum class Code { kOk, kIOError, kCorrupt, kNotFound, kInvalidArg };
  Code code_ = Code::kOk;
  std::string msg_;
};

}  // namespace mini_hadoop

#define MINI_HADOOP_VERSION "0.1.0"
