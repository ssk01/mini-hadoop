#include <gtest/gtest.h>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <thread>

#include "ndfs/name_node.h"
#include "ndfs/data_node.h"
#include "ndfs/dfs_client.h"
#include "mapred/dist_job_tracker.h"
#include "mapred/task_tracker.h"
#include "mapred/examples/wordcount.h"

using namespace mini_hadoop;

TEST(FullDistChainTest, NdfsBackedDistributedMR) {
  std::filesystem::remove_all("/tmp/mini-hadoop-distchain");

  // Start NDFS
  ndfs::NameNode nn(17210, "/tmp/mini-hadoop-distchain/fs");
  ASSERT_TRUE(nn.Start());
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  ndfs::DataNode dn("/tmp/mini-hadoop-distchain/dn", "127.0.0.1", 17210, 17220);
  ASSERT_TRUE(dn.Start());
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // Upload input via NDFS
  ndfs::DfsClient dfs("127.0.0.1", 17210);
  ASSERT_TRUE(dfs.Connect().ok());

  std::string input_text;
  for (int i = 0; i < 5000; i++) input_text += "hello world hello\n";
  ASSERT_TRUE(dfs.WriteFile("/mrinput.txt",
        reinterpret_cast<const uint8_t*>(input_text.data()),
        input_text.size(), true).ok());

  // Download for local MR processing
  std::vector<uint8_t> raw;
  ASSERT_TRUE(dfs.ReadFile("/mrinput.txt", raw).ok());
  std::string local_in = "/tmp/mini-hadoop-distchain/lin.txt";
  std::string local_out = "/tmp/mini-hadoop-distchain/lout.txt";
  {
    std::ofstream f(local_in);
    f.write(reinterpret_cast<const char*>(raw.data()), raw.size());
  }

  // Start distributed MR
  mapred::DistJobTracker jt(17230,
      []() { return std::make_unique<examples::WordCountMapper>(); },
      []() { return std::make_unique<examples::WordCountReducer>(); });
  ASSERT_TRUE(jt.Start());
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  { // Submit job
    ipc::RpcClient c;
    ASSERT_TRUE(c.Connect("127.0.0.1", 17230));
    OutputBuffer req;
    req.WriteString(local_in);
    req.WriteString(local_out);
    req.WriteInt(1); req.WriteInt(1);
    std::vector<uint8_t> resp;
    ASSERT_TRUE(c.CallRaw(0, req.Data(), resp).ok());
    c.Disconnect();
  }

  mapred::TaskTracker tt("127.0.0.1", 17230, "127.0.0.1", 17231,
      []() { return std::make_unique<examples::WordCountMapper>(); },
      []() { return std::make_unique<examples::WordCountReducer>(); });
  ASSERT_TRUE(tt.Start());

  int waited = 0;
  while (!jt.IsJobDone() && waited < 30) {
    std::this_thread::sleep_for(std::chrono::seconds(1)); waited++;
  }
  EXPECT_TRUE(jt.IsJobDone()) << "Job did not complete";
  if (!jt.IsJobDone()) { tt.Stop(); jt.Stop(); return; }

  // Verify output file contains correct results
  std::ifstream rf(local_out);
  std::string line;
  bool has_hello = false, has_world = false;
  while (std::getline(rf, line)) {
    if (line.find("hello\t10000") != std::string::npos) has_hello = true;
    if (line.find("world\t5000") != std::string::npos) has_world = true;
  }
  EXPECT_TRUE(has_hello);
  EXPECT_TRUE(has_world);

  tt.Stop(); jt.Stop();
  dfs.Disconnect(); dn.Stop(); nn.Stop();
}
