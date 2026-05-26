#include <gtest/gtest.h>
#include "io/writable_types.h"

using namespace mini_hadoop;

TEST(WritableTypesTest, IntWritableRoundTrip) {
  OutputBuffer out;
  IntWritable(42).Write(out);

  InputBuffer in;
  in.Reset(out.Data());
  IntWritable v;
  v.ReadFields(in);
  EXPECT_EQ(v.Get(), 42);
}

TEST(WritableTypesTest, IntWritableCompare) {
  IntWritable a(10), b(20), c(10);
  EXPECT_LT(a.CompareTo(b), 0);
  EXPECT_GT(b.CompareTo(a), 0);
  EXPECT_EQ(a.CompareTo(c), 0);
}

TEST(WritableTypesTest, LongWritableRoundTrip) {
  OutputBuffer out;
  LongWritable(0xDEADBEEF).Write(out);

  InputBuffer in;
  in.Reset(out.Data());
  LongWritable v;
  v.ReadFields(in);
  EXPECT_EQ(v.Get(), 0xDEADBEEF);
}

TEST(WritableTypesTest, LongWritableCompare) {
  LongWritable a(5), b(10);
  EXPECT_TRUE(a < b);
  EXPECT_FALSE(b < a);
}

TEST(WritableTypesTest, FloatWritableRoundTrip) {
  OutputBuffer out;
  FloatWritable(1.5f).Write(out);

  InputBuffer in;
  in.Reset(out.Data());
  FloatWritable v;
  v.ReadFields(in);
  EXPECT_FLOAT_EQ(v.Get(), 1.5f);
}

TEST(WritableTypesTest, BooleanWritableRoundTrip) {
  OutputBuffer out;
  BooleanWritable(true).Write(out);
  BooleanWritable(false).Write(out);

  InputBuffer in;
  in.Reset(out.Data());
  BooleanWritable v;
  v.ReadFields(in);
  EXPECT_TRUE(v.Get());
  v.ReadFields(in);
  EXPECT_FALSE(v.Get());
}

TEST(WritableTypesTest, UTF8RoundTrip) {
  OutputBuffer out;
  UTF8("hello world").Write(out);

  InputBuffer in;
  in.Reset(out.Data());
  UTF8 v;
  v.ReadFields(in);
  EXPECT_EQ(v.Get(), "hello world");
}

TEST(WritableTypesTest, UTF8Compare) {
  UTF8 a("apple"), b("banana"), c("apple");
  EXPECT_LT(a.CompareTo(b), 0);
  EXPECT_EQ(a.CompareTo(c), 0);
  EXPECT_GT(b.CompareTo(a), 0);
}

TEST(WritableTypesTest, UTF8Empty) {
  OutputBuffer out;
  UTF8("").Write(out);

  InputBuffer in;
  in.Reset(out.Data());
  UTF8 v;
  v.ReadFields(in);
  EXPECT_EQ(v.Get(), "");
}

TEST(WritableTypesTest, BytesWritableRoundTrip) {
  std::vector<uint8_t> data = {1, 2, 3, 4, 5};
  OutputBuffer out;
  BytesWritable(data).Write(out);

  InputBuffer in;
  in.Reset(out.Data());
  BytesWritable v;
  v.ReadFields(in);
  EXPECT_EQ(v.Get(), data);
}

TEST(WritableTypesTest, NullWritableRoundTrip) {
  OutputBuffer out;
  NullWritable().Write(out);
  EXPECT_EQ(out.Size(), 0);

  InputBuffer in;
  uint8_t d = 0;
  in.Reset(&d, 1);
  NullWritable n;
  n.ReadFields(in);
  EXPECT_EQ(in.Position(), 0);  // no bytes consumed
}
