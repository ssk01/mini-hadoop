#include <gtest/gtest.h>
#include <cstdio>
#include "io/sequence_file.h"
#include "io/writable_types.h"

using namespace mini_hadoop;

const std::string kTestFile = "/tmp/mini-hadoop-seq-test.seq";

TEST(SequenceFileTest, WriteAndReadBack) {
  SequenceFileWriter writer;
  ASSERT_TRUE(writer.Open(kTestFile).ok());

  for (int i = 0; i < 50; i++) {
    ASSERT_TRUE(writer.Append(IntWritable(i), LongWritable(i * 100)).ok());
  }
  writer.Close();
  EXPECT_EQ(writer.Count(), 50);

  SequenceFileReader reader;
  ASSERT_TRUE(reader.Open(kTestFile).ok());

  int count = 0;
  while (reader.Next()) {
    auto kdata = reader.Key();
    auto vdata = reader.Value();

    InputBuffer kb(kdata), vb(vdata);
    IntWritable k;
    k.ReadFields(kb);
    LongWritable v;
    v.ReadFields(vb);

    EXPECT_EQ(k.Get(), count);
    EXPECT_EQ(v.Get(), static_cast<int64_t>(count) * 100);
    count++;
  }
  reader.Close();
  EXPECT_EQ(count, 50);

  std::remove(kTestFile.c_str());
}

TEST(SequenceFileTest, SyncMarker) {
  SequenceFileWriter writer;
  ASSERT_TRUE(writer.Open(kTestFile).ok());

  for (int i = 0; i < 250; i++) {
    ASSERT_TRUE(writer.Append(IntWritable(i), IntWritable(i)).ok());
  }
  writer.Close();

  SequenceFileReader reader;
  ASSERT_TRUE(reader.Open(kTestFile).ok());

  int count = 0;
  while (reader.Next()) {
    InputBuffer k(reader.Key());
    IntWritable kw;
    kw.ReadFields(k);
    EXPECT_EQ(kw.Get(), count);
    count++;
  }
  reader.Close();
  EXPECT_EQ(count, 250);

  std::remove(kTestFile.c_str());
}

TEST(SequenceFileTest, EmptyKeyRejected) {
  SequenceFileWriter writer;
  ASSERT_TRUE(writer.Open(kTestFile).ok());

  auto s = writer.AppendRaw(std::span<const uint8_t>{}, std::span<const uint8_t>{1, 2, 3});
  EXPECT_FALSE(s.ok());
  EXPECT_EQ(s.message(), "zero-length key");

  writer.Close();
  std::remove(kTestFile.c_str());
}
