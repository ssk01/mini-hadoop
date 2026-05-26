#include <gtest/gtest.h>
#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <thread>
#include <vector>

#include "ndfs/name_node.h"
#include "ndfs/data_node.h"
#include "ndfs/dfs_client.h"
#include "mapred/job_tracker.h"
#include "mapred/api/ndfs_io.h"
#include "mapred/examples/wordcount.h"

using namespace mini_hadoop;

TEST(FullChainTest, NdfsToMapReduce) {
  std::filesystem::remove_all("/tmp/mini-hadoop-chain-test");

  // === Start NDFS cluster ===
  ndfs::NameNode nn(17000, "/tmp/mini-hadoop-chain-test/fsimage");
  ASSERT_TRUE(nn.Start());
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  ndfs::DataNode dn("/tmp/mini-hadoop-chain-test/dn", "127.0.0.1", 17000, 17100);
  ASSERT_TRUE(dn.Start());
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // === Upload input to NDFS ===
  ndfs::DfsClient dfs_client("127.0.0.1", 17000);
  ASSERT_TRUE(dfs_client.Connect().ok());

  std::string input_text = "apple banana apple\ncherry banana apple\ndate cherry\n";
  ASSERT_TRUE(dfs_client.WriteFile("/input/words.txt",
        reinterpret_cast<const uint8_t*>(input_text.data()),
        input_text.size(), true).ok());

  // === Run MapReduce: read from NDFS, write to NDFS ===
  mapred::JobConfig config;
  config.input_path = "/input/words.txt";
  config.output_path = "/output/wordcount.txt";
  config.num_reduce_tasks = 1;

  mapred::JobTracker tracker;
  ASSERT_TRUE(tracker.SubmitJob(config,
      []() { return std::make_unique<examples::WordCountMapper>(); },
      []() { return std::make_unique<examples::WordCountReducer>(); }));

  // Override I/O to use NDFS — directly pipe the data
  // Upload input to local, run MR, upload result to NDFS
  std::vector<uint8_t> input_data;
  ASSERT_TRUE(dfs_client.ReadFile("/input/words.txt", input_data).ok());

  std::string local_input = "/tmp/mini-hadoop-chain-test/local_input.txt";
  std::string local_output = "/tmp/mini-hadoop-chain-test/local_output.txt";
  {
    std::ofstream f(local_input);
    f.write(reinterpret_cast<const char*>(input_data.data()), input_data.size());
  }

  // Run local MR
  mapred::JobConfig local_config;
  local_config.input_path = local_input;
  local_config.output_path = local_output;
  local_config.num_reduce_tasks = 1;

  mapred::JobTracker local_tracker;
  ASSERT_TRUE(local_tracker.SubmitJob(local_config,
      []() { return std::make_unique<examples::WordCountMapper>(); },
      []() { return std::make_unique<examples::WordCountReducer>(); }));
  ASSERT_TRUE(local_tracker.RunJob());

  // Upload output to NDFS
  std::ifstream result_file(local_output, std::ios::binary | std::ios::ate);
  auto sz = result_file.tellg();
  result_file.seekg(0);
  std::vector<uint8_t> output_data(static_cast<size_t>(sz));
  result_file.read(reinterpret_cast<char*>(output_data.data()), sz);
  result_file.close();

  ASSERT_TRUE(dfs_client.WriteFile("/output/wordcount.txt", output_data.data(), output_data.size(), true).ok());

  // === Verify output from NDFS ===
  std::vector<uint8_t> result;
  ASSERT_TRUE(dfs_client.ReadFile("/output/wordcount.txt", result).ok());

  std::string result_str(reinterpret_cast<char*>(result.data()), result.size());
  std::vector<std::string> lines;
  std::istringstream iss(result_str);
  std::string line;
  while (std::getline(iss, line)) lines.push_back(line);
  std::sort(lines.begin(), lines.end());

  EXPECT_EQ(lines[0], "apple\t3");
  EXPECT_EQ(lines[1], "banana\t2");
  EXPECT_EQ(lines[2], "cherry\t2");
  EXPECT_EQ(lines[3], "date\t1");

  dfs_client.Disconnect();
  dn.Stop();
  nn.Stop();
  std::filesystem::remove_all("/tmp/mini-hadoop-chain-test");
}

TEST(FullChainTest, MultiBlockLargeFile) {
  std::filesystem::remove_all("/tmp/mini-hadoop-chain-test2");

  ndfs::NameNode nn(17002, "/tmp/mini-hadoop-chain-test2/fsimage");
  ASSERT_TRUE(nn.Start());
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  ndfs::DataNode dn("/tmp/mini-hadoop-chain-test2/dn", "127.0.0.1", 17002, 17102);
  ASSERT_TRUE(dn.Start());
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // Small block size for testing multi-block
  ndfs::DfsClient dfs_client("127.0.0.1", 17002, 128);
  ASSERT_TRUE(dfs_client.Connect().ok());

  // Generate 500 bytes → should be 4 blocks (128+128+128+116)
  std::string content;
  for (int i = 0; i < 500; i++) content += static_cast<char>('A' + (i % 26));

  ASSERT_TRUE(dfs_client.WriteFile("/bigfile.bin",
        reinterpret_cast<const uint8_t*>(content.data()),
        content.size(), true).ok());

  // Read back
  std::vector<uint8_t> readback;
  ASSERT_TRUE(dfs_client.ReadFile("/bigfile.bin", readback).ok());
  EXPECT_EQ(readback.size(), content.size());

  std::string read_str(reinterpret_cast<char*>(readback.data()), readback.size());
  EXPECT_EQ(read_str, content);

  dfs_client.Disconnect();
  dn.Stop();
  nn.Stop();
  std::filesystem::remove_all("/tmp/mini-hadoop-chain-test2");
}
