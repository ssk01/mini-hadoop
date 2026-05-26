#pragma once

#include <fstream>
#include <vector>
#include "io/writable_types.h"
#include "mapred/api/input_format.h"
#include "ndfs/dfs_client.h"

namespace mini_hadoop {
namespace mapred {

class NdfsLineRecordReader : public RecordReader<LongWritable, UTF8> {
 public:
  NdfsLineRecordReader(ndfs::DfsClient* client, const std::string& path)
      : client_(client), path_(path) {
    auto st = client_->ReadFile(path_, file_data_);
    if (st.ok()) {
      // Split by lines
      std::string content(reinterpret_cast<char*>(file_data_.data()), file_data_.size());
      std::istringstream iss(content);
      std::string line;
      while (std::getline(iss, line)) lines_.push_back(line);
    }
  }

  bool Next(LongWritable& key, UTF8& value) override {
    if (pos_ >= lines_.size()) return false;
    key.Set(static_cast<int64_t>(pos_));
    value.Set(lines_[pos_]);
    pos_++;
    return true;
  }

  float GetProgress() override {
    return lines_.empty() ? 1.0f : static_cast<float>(pos_) / lines_.size();
  }

  void Close() override {}

 private:
  ndfs::DfsClient* client_;
  std::string path_;
  std::vector<uint8_t> file_data_;
  std::vector<std::string> lines_;
  size_t pos_ = 0;
};

class NdfsInputFormat : public InputFormat<LongWritable, UTF8> {
 public:
  explicit NdfsInputFormat(ndfs::DfsClient* client) : client_(client) {}

  std::unique_ptr<RecordReader<LongWritable, UTF8>> CreateReader(
      const std::string& path) override {
    return std::make_unique<NdfsLineRecordReader>(client_, path);
  }

 private:
  ndfs::DfsClient* client_;
};

class NdfsOutputFormat : public OutputFormat<UTF8, LongWritable> {
 public:
  explicit NdfsOutputFormat(ndfs::DfsClient* client) : client_(client) {}

  std::unique_ptr<RecordWriter<UTF8, LongWritable>> CreateWriter(
      const std::string& path) override {
    return std::make_unique<NdfsRecordWriter>(client_, path);
  }

 private:
  class NdfsRecordWriter : public RecordWriter<UTF8, LongWritable> {
   public:
    NdfsRecordWriter(ndfs::DfsClient* client, const std::string& path)
        : client_(client), path_(path) {}

    void Write(const UTF8& key, const LongWritable& value) override {
      std::string line = key.Get() + "\t" + std::to_string(value.Get()) + "\n";
      buffer_.insert(buffer_.end(),
                     reinterpret_cast<const uint8_t*>(line.data()),
                     reinterpret_cast<const uint8_t*>(line.data() + line.size()));
    }

    void Close() override {
      client_->WriteFile(path_, buffer_.data(), buffer_.size(), true);
    }

   private:
    ndfs::DfsClient* client_;
    std::string path_;
    std::vector<uint8_t> buffer_;
  };

  ndfs::DfsClient* client_;
};

}  // namespace mapred
}  // namespace mini_hadoop
