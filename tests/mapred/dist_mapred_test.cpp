#include <gtest/gtest.h>
#include <chrono>
#include <cstdio>
#include <fstream>
#include <set>
#include <thread>

#include "mapred/dist_job_tracker.h"
#include "mapred/task_tracker.h"
#include "mapred/examples/wordcount.h"

using namespace mini_hadoop;

TEST(DistMapReduceTest, WordCountDistributed) {
  std::string input_file = "/tmp/mini-hadoop-mr-dist-input.txt";

  {
    std::ofstream f(input_file);
    f << "hello world\n";
    f << "hello mapreduce\n";
  }

  // Start JobTracker
  mapred::DistJobTracker jt(18000,
      []() { return std::make_unique<examples::WordCountMapper>(); },
      []() { return std::make_unique<examples::WordCountReducer>(); });
  ASSERT_TRUE(jt.Start());
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Submit job
  {
    mini_hadoop::ipc::RpcClient client;
    ASSERT_TRUE(client.Connect("127.0.0.1", 18000));

    mini_hadoop::OutputBuffer req;
    req.WriteString(input_file);
    req.WriteString("/tmp/mr-out.txt");
    req.WriteInt(1);  // num map tasks
    req.WriteInt(1);  // num reduce tasks

    std::vector<uint8_t> resp;
    ASSERT_TRUE(client.CallRaw(0, req.Data(), resp).ok());
    client.Disconnect();
  }

  // Start 2 TaskTrackers
  mapred::TaskTracker tt1("127.0.0.1", 18000, "127.0.0.1", 18001,
      []() { return std::make_unique<examples::WordCountMapper>(); },
      []() { return std::make_unique<examples::WordCountReducer>(); });

  mapred::TaskTracker tt2("127.0.0.1", 18000, "127.0.0.1", 18002,
      []() { return std::make_unique<examples::WordCountMapper>(); },
      []() { return std::make_unique<examples::WordCountReducer>(); });

  ASSERT_TRUE(tt1.Start());
  ASSERT_TRUE(tt2.Start());

  // Wait for job completion
  int waited = 0;
  while (!jt.IsJobDone() && waited < 30) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    waited++;
  }
  EXPECT_TRUE(jt.IsJobDone());

  tt1.Stop();
  tt2.Stop();
  jt.Stop();

  std::remove(input_file.c_str());
}
