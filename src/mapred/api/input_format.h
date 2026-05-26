#pragma once

#include <string>
#include <vector>
#include <fstream>
#include <sstream>
#include "io/buffer.h"
#include "io/writable_types.h"

namespace mini_hadoop {
namespace mapred {

template <typename K, typename V>
class RecordReader {
 public:
  virtual ~RecordReader() = default;
  virtual bool Next(K& key, V& value) = 0;
  virtual float GetProgress() = 0;
  virtual void Close() = 0;
};

template <typename K, typename V>
class RecordWriter {
 public:
  virtual ~RecordWriter() = default;
  virtual void Write(const K& key, const V& value) = 0;
  virtual void Close() = 0;
};

template <typename K, typename V>
class InputFormat {
 public:
  virtual ~InputFormat() = default;
  virtual std::unique_ptr<RecordReader<K, V>> CreateReader(
      const std::string& path) = 0;
};

template <typename K, typename V>
class OutputFormat {
 public:
  virtual ~OutputFormat() = default;
  virtual std::unique_ptr<RecordWriter<K, V>> CreateWriter(
      const std::string& path) = 0;
};

class TextRecordReader : public RecordReader<LongWritable, UTF8> {
 public:
  explicit TextRecordReader(const std::string& path) : file_(path) {}

  bool Next(LongWritable& key, UTF8& value) override {
    if (pos_ == 0 && !std::getline(file_, current_line_)) return false;
    if (pos_ > 0 && !std::getline(file_, current_line_)) return false;

    key.Set(pos_);
    value.Set(current_line_);
    pos_ += static_cast<int64_t>(current_line_.size()) + 1;
    return true;
  }

  float GetProgress() override { return file_.eof() ? 1.0f : 0.0f; }
  void Close() override { file_.close(); }

 private:
  std::ifstream file_;
  int64_t pos_ = 0;
  std::string current_line_;
};

class TextInputFormat : public InputFormat<LongWritable, UTF8> {
 public:
  std::unique_ptr<RecordReader<LongWritable, UTF8>> CreateReader(
      const std::string& path) override {
    return std::make_unique<TextRecordReader>(path);
  }
};

class TextRecordWriter : public RecordWriter<UTF8, UTF8> {
 public:
  explicit TextRecordWriter(const std::string& path) : file_(path) {}

  void Write(const UTF8& key, const UTF8& value) override {
    file_ << key.Get() << "\t" << value.Get() << "\n";
  }

  void Close() override { file_.close(); }

 private:
  std::ofstream file_;
};

class TextOutputFormat : public OutputFormat<UTF8, UTF8> {
 public:
  std::unique_ptr<RecordWriter<UTF8, UTF8>> CreateWriter(
      const std::string& path) override {
    return std::make_unique<TextRecordWriter>(path);
  }
};

}  // namespace mapred
}  // namespace mini_hadoop
