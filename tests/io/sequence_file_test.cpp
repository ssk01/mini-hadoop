#include <gtest/gtest.h>
#include <cstdio>
#include "io/sequence_file.h"
#include "io/writable_types.h"

using namespace mini_hadoop;

TEST(SequenceFileTest, WriteAndReadBack) {
  SequenceFileWriter w;
  ASSERT_TRUE(w.Open("/tmp/mh-seq-test.seq").ok());

  for (int i = 0; i < 50; i++)
    ASSERT_TRUE(w.Append(IntWritable(i), LongWritable(i * 100)).ok());
  w.Close();

  SequenceFileReader r;
  ASSERT_TRUE(r.Open("/tmp/mh-seq-test.seq").ok());

  int count = 0;
  while (r.Next()) {
    InputBuffer k(r.Key()), v(r.Value());
    IntWritable ik; ik.ReadFields(k);
    LongWritable lv; lv.ReadFields(v);
    EXPECT_EQ(ik.Get(), count);
    EXPECT_EQ(lv.Get(), static_cast<int64_t>(count) * 100);
    count++;
  }
  EXPECT_EQ(count, 50);
  r.Close();
  std::remove("/tmp/mh-seq-test.seq");
}

TEST(SequenceFileTest, SyncMarkers) {
  SequenceFileWriter w;
  ASSERT_TRUE(w.Open("/tmp/mh-seq-sync.seq").ok());
  for (int i = 0; i < 250; i++)
    ASSERT_TRUE(w.Append(IntWritable(i), IntWritable(i)).ok());
  w.Close();

  SequenceFileReader r;
  ASSERT_TRUE(r.Open("/tmp/mh-seq-sync.seq").ok());
  int count = 0;
  while (r.Next()) { count++; }
  EXPECT_EQ(count, 250);
  r.Close();
  std::remove("/tmp/mh-seq-sync.seq");
}

TEST(SequenceFileTest, EmptyKeyRejected) {
  SequenceFileWriter w;
  ASSERT_TRUE(w.Open("/tmp/mh-seq-empty.seq").ok());
  EXPECT_FALSE(w.AppendRaw({}, {}).ok());
  w.Close();
  std::remove("/tmp/mh-seq-empty.seq");
}
