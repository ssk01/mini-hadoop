#include <gtest/gtest.h>
#include <cstdio>
#include <fstream>
#include <filesystem>
#include "mapred/job_tracker.h"
#include "mapred/examples/wordcount.h"

using namespace mini_hadoop;

TEST(MapReduceTest, WordCount) {
  std::string input_file = "/tmp/mini-hadoop-mr-input.txt";
  std::string output_file = "/tmp/mini-hadoop-mr-output.txt";

  // Write test input
  {
    std::ofstream f(input_file);
    f << "hello world\n";
    f << "hello mapreduce\n";
    f << "world of testing\n";
  }

  mapred::JobConfig config;
  config.input_path = input_file;
  config.output_path = output_file;
  config.num_reduce_tasks = 2;

  mapred::JobTracker tracker;
  ASSERT_TRUE(tracker.SubmitJob(config,
      []() { return std::make_unique<examples::WordCountMapper>(); },
      []() { return std::make_unique<examples::WordCountReducer>(); }));

  ASSERT_TRUE(tracker.RunJob());
  ASSERT_TRUE(tracker.IsJobDone());

  // Verify output
  std::ifstream result(output_file);
  std::string line;
  std::vector<std::string> lines;
  while (std::getline(result, line)) lines.push_back(line);

  std::sort(lines.begin(), lines.end());

  EXPECT_EQ(lines[0], "hello\t2");
  EXPECT_EQ(lines[1], "mapreduce\t1");
  EXPECT_EQ(lines[2], "of\t1");
  EXPECT_EQ(lines[3], "testing\t1");
  EXPECT_EQ(lines[4], "world\t2");

  std::remove(input_file.c_str());
  std::remove(output_file.c_str());
}

TEST(MapReduceTest, SingleWordInput) {
  std::string input_file = "/tmp/mini-hadoop-mr-input2.txt";
  std::string output_file = "/tmp/mini-hadoop-mr-output2.txt";

  {
    std::ofstream f(input_file);
    f << "aaa aaa aaa\n";
  }

  mapred::JobConfig config;
  config.input_path = input_file;
  config.output_path = output_file;
  config.num_reduce_tasks = 1;

  mapred::JobTracker tracker;
  ASSERT_TRUE(tracker.SubmitJob(config,
      []() { return std::make_unique<examples::WordCountMapper>(); },
      []() { return std::make_unique<examples::WordCountReducer>(); }));

  ASSERT_TRUE(tracker.RunJob());

  std::ifstream result(output_file);
  std::string line;
  ASSERT_TRUE(std::getline(result, line));
  EXPECT_EQ(line, "aaa\t3");

  std::remove(input_file.c_str());
  std::remove(output_file.c_str());
}

TEST(MapReduceTest, CombinerReducesOutput) {
  std::string input_file = "/tmp/mini-hadoop-mr-combiner.txt";
  std::string output_file = "/tmp/mini-hadoop-mr-combiner-out.txt";

  {
    std::ofstream f(input_file);
    for (int i = 0; i < 1000; i++) f << "aaa bbb aaa\n";
  }

  mapred::JobConfig config;
  config.input_path = input_file;
  config.output_path = output_file;
  config.num_reduce_tasks = 1;

  mapred::JobTracker tracker;
  ASSERT_TRUE(tracker.SubmitJob(config,
      []() { return std::make_unique<examples::WordCountMapper>(); },
      []() { return std::make_unique<examples::WordCountReducer>(); },
      []() { return std::make_unique<examples::WordCountCombiner>(); }));

  ASSERT_TRUE(tracker.RunJob());

  std::ifstream result(output_file);
  std::map<std::string, int> counts;
  std::string line;
  while (std::getline(result, line)) {
    std::istringstream iss(line);
    std::string w; int c;
    iss >> w >> c;
    counts[w] = c;
  }
  EXPECT_EQ(counts["aaa"], 2000);
  EXPECT_EQ(counts["bbb"], 1000);

  std::remove(input_file.c_str());
  std::remove(output_file.c_str());
}
