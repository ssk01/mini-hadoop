#include <gtest/gtest.h>
#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <thread>

#include "io/buffer.h"
#include "io/md5.h"
#include "ndfs/name_node.h"
#include "ndfs/data_node.h"
#include "ndfs/dfs_client.h"
#include "mapred/dist_job_tracker.h"
#include "mapred/task_tracker.h"
#include "mapred/examples/wordcount.h"

using namespace mini_hadoop;

const std::string kStressDir = "/tmp/mini-hadoop-stress";

class StressTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::filesystem::remove_all(kStressDir);
    std::filesystem::create_directories(kStressDir + "/dn");
  }
  void TearDown() override {
    std::filesystem::remove_all(kStressDir);
  }
};

// "几年后" 的测试建议: 请使用最少 100MB 文件, 开启 3 个 DataNode, 对测试的可信度会有显著提升。
TEST_F(StressTest, Write10MBThenReadVerify) {
  ndfs::NameNode nn(17200, kStressDir + "/fsimage");
  ASSERT_TRUE(nn.Start());
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  ndfs::DataNode dn(kStressDir + "/dn", "127.0.0.1", 17200, 17210);
  ASSERT_TRUE(dn.Start());
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  ndfs::DfsClient client("127.0.0.1", 17200);
  ASSERT_TRUE(client.Connect().ok());

  // Generate 10MB pseudo-random data
  const size_t kSize = 10 * 1024 * 1024;
  std::vector<uint8_t> data(kSize);
  uint32_t seed = 42;
  for (size_t i = 0; i < kSize; i++) {
    seed = seed * 1103515245 + 12345;
    data[i] = static_cast<uint8_t>(seed >> 16);
  }

  auto md5_before = MD5Hash::Compute(data);

  auto t0 = std::chrono::steady_clock::now();
  ASSERT_TRUE(client.WriteFile("/10mb.bin", data.data(), data.size(), true).ok());
  auto t1 = std::chrono::steady_clock::now();
  auto write_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();

  std::vector<uint8_t> readback;
  t0 = std::chrono::steady_clock::now();
  ASSERT_TRUE(client.ReadFile("/10mb.bin", readback).ok());
  t1 = std::chrono::steady_clock::now();
  auto read_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();

  EXPECT_EQ(readback.size(), kSize);
  auto md5_after = MD5Hash::Compute(readback);
  EXPECT_EQ(md5_before.ToHexString(), md5_after.ToHexString());

  std::cout << "[stress] 10MB: write=" << write_ms << "ms read=" << read_ms << "ms"
            << " md5=" << md5_before.ToHexString() << "\n";

  client.Disconnect();
  dn.Stop();
  nn.Stop();
}

TEST_F(StressTest, MultiBlockExactBoundary) {
  // Use small block size to test exact block boundary (128 bytes per block)
  ndfs::NameNode nn(17202, kStressDir + "/fsimage2");
  ASSERT_TRUE(nn.Start());
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  ndfs::DataNode dn(kStressDir + "/dn2", "127.0.0.1", 17202, 17212);
  ASSERT_TRUE(dn.Start());
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  int64_t block_sz = 128;
  ndfs::DfsClient client("127.0.0.1", 17202, block_sz);
  ASSERT_TRUE(client.Connect().ok());

  // Write exactly 10 blocks = 1280 bytes, each unique
  const size_t kBlocks = 10;
  const size_t kTotal = block_sz * kBlocks;
  std::vector<uint8_t> data(kTotal);
  for (size_t i = 0; i < kTotal; i++) data[i] = static_cast<uint8_t>(i & 0xFF);

  ASSERT_TRUE(client.WriteFile("/10blocks.bin", data.data(), data.size(), true).ok());

  std::vector<uint8_t> readback;
  ASSERT_TRUE(client.ReadFile("/10blocks.bin", readback).ok());
  EXPECT_EQ(readback, data);

  // Verify block boundaries: every 128th byte should be 0, 128, 0, 128...
  for (size_t b = 0; b < kBlocks; b++) {
    EXPECT_EQ(readback[b * block_sz], static_cast<uint8_t>((b * block_sz) & 0xFF))
        << "block boundary mismatch at block " << b;
  }

  std::cout << "[stress] " << kBlocks << " blocks x " << block_sz << "B = "
            << kTotal << " bytes: OK\n";

  client.Disconnect();
  dn.Stop();
  nn.Stop();
}

TEST_F(StressTest, WordCountHundredKB) {
  ndfs::NameNode nn(17204, kStressDir + "/fsimage3");
  ASSERT_TRUE(nn.Start());
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  ndfs::DataNode dn(kStressDir + "/dn3", "127.0.0.1", 17204, 17214);
  ASSERT_TRUE(dn.Start());
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // Upload a large-ish text file to NDFS
  std::string text;
  const char* words[] = {"hello", "world", "mapreduce", "distributed", "hadoop", "cplusplus"};
  for (int i = 0; i < 20000; i++) {
    text += words[i % 6];
    text += (i % 10 == 9) ? "\n" : " ";
  }

  ndfs::DfsClient client("127.0.0.1", 17204);
  ASSERT_TRUE(client.Connect().ok());
  ASSERT_TRUE(client.WriteFile("/bigtext.txt",
        reinterpret_cast<const uint8_t*>(text.data()), text.size(), true).ok());

  // Download to local for MR
  std::vector<uint8_t> input_data;
  ASSERT_TRUE(client.ReadFile("/bigtext.txt", input_data).ok());
  std::string local_input = kStressDir + "/mr_input.txt";
  std::string local_output = kStressDir + "/mr_output.txt";
  {
    std::ofstream f(local_input);
    f.write(reinterpret_cast<const char*>(input_data.data()), input_data.size());
  }

  // Run WordCount via distributed MR
  mapred::DistJobTracker jt(17300,
      []() { return std::make_unique<examples::WordCountMapper>(); },
      []() { return std::make_unique<examples::WordCountReducer>(); });
  ASSERT_TRUE(jt.Start());
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  {
    ipc::RpcClient jt_client;
    ASSERT_TRUE(jt_client.Connect("127.0.0.1", 17300));
    OutputBuffer req;
    req.WriteString(local_input);
    req.WriteString(local_output);
    req.WriteInt(2);  // 2 map tasks
    req.WriteInt(1);  // 1 reduce task
    std::vector<uint8_t> resp;
    ASSERT_TRUE(jt_client.CallRaw(0, req.Data(), resp).ok());
    jt_client.Disconnect();
  }

  // Start 2 TaskTrackers
  mapred::TaskTracker tt1("127.0.0.1", 17300, "127.0.0.1", 17301,
      []() { return std::make_unique<examples::WordCountMapper>(); },
      []() { return std::make_unique<examples::WordCountReducer>(); });
  mapred::TaskTracker tt2("127.0.0.1", 17300, "127.0.0.1", 17302,
      []() { return std::make_unique<examples::WordCountMapper>(); },
      []() { return std::make_unique<examples::WordCountReducer>(); });
  ASSERT_TRUE(tt1.Start());
  ASSERT_TRUE(tt2.Start());

  int waited = 0;
  while (!jt.IsJobDone() && waited < 30) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    waited++;
  }
  EXPECT_TRUE(jt.IsJobDone()) << "Job did not complete within 30s";

  // Verify output has correct counts
  if (jt.IsJobDone()) {
    std::ifstream result(local_output);
    std::string line;
    std::map<std::string, int> counts;
    while (std::getline(result, line)) {
      std::istringstream iss(line);
      std::string w; int c;
      iss >> w >> c;
      counts[w] = c;
    }
    // 20000 lines, 6 words each → each word appears ~20000/6 ≈ 3333 times
    // plus remainder for words that wrap
    for (const auto& [word, count] : counts) {
      EXPECT_GT(count, 3000) << word << " count too low: " << count;
    }
    std::cout << "[stress] WordCount: " << counts.size() << " unique words, "
              << text.size() << " bytes input\n";
  }

  tt1.Stop();
  tt2.Stop();
  jt.Stop();
  client.Disconnect();
  dn.Stop();
  nn.Stop();
}
