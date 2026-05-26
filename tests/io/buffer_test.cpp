#include <gtest/gtest.h>
#include "io/buffer.h"

using namespace mini_hadoop;

TEST(BufferTest, WriteReadInt) {
  OutputBuffer out;
  out.WriteInt(42);
  out.WriteInt(-1);
  out.WriteInt(0x7FFFFFFF);

  InputBuffer in;
  in.Reset(out.Data());

  EXPECT_EQ(in.ReadInt(), 42);
  EXPECT_EQ(in.ReadInt(), -1);
  EXPECT_EQ(in.ReadInt(), 0x7FFFFFFF);
  EXPECT_EQ(in.Remaining(), 0);
}

TEST(BufferTest, WriteReadLong) {
  OutputBuffer out;
  out.WriteLong(0x1122334455667788LL);

  InputBuffer in;
  in.Reset(out.Data());

  EXPECT_EQ(in.ReadLong(), 0x1122334455667788LL);
}

TEST(BufferTest, WriteReadFloat) {
  OutputBuffer out;
  out.WriteFloat(3.14f);

  InputBuffer in;
  in.Reset(out.Data());

  EXPECT_FLOAT_EQ(in.ReadFloat(), 3.14f);
}

TEST(BufferTest, WriteReadDouble) {
  OutputBuffer out;
  out.WriteDouble(2.718281828);

  InputBuffer in;
  in.Reset(out.Data());

  EXPECT_DOUBLE_EQ(in.ReadDouble(), 2.718281828);
}

TEST(BufferTest, WriteReadString) {
  OutputBuffer out;
  out.WriteString("hello world");

  InputBuffer in;
  in.Reset(out.Data());

  EXPECT_EQ(in.ReadString(), "hello world");
}

TEST(BufferTest, WriteReadBytes) {
  OutputBuffer out;
  std::vector<uint8_t> data = {0xDE, 0xAD, 0xBE, 0xEF};
  out.WriteShort(16);
  out.WriteBytes(data);

  InputBuffer in;
  in.Reset(out.Data());

  EXPECT_EQ(in.ReadShort(), 16);
  EXPECT_EQ(in.ReadBytes(), data);
}

TEST(BufferTest, ResetReuse) {
  OutputBuffer out;
  out.WriteInt(100);
  auto len1 = out.Size();

  out.Reset();
  out.WriteInt(200);
  auto len2 = out.Size();

  EXPECT_EQ(len1, len2);
  InputBuffer in;
  in.Reset(out.Data());
  EXPECT_EQ(in.ReadInt(), 200);
}

TEST(BufferTest, ReadPastEndThrows) {
  InputBuffer in;
  uint8_t d = 0;
  in.Reset(&d, 1);
  EXPECT_NO_THROW(in.ReadByte());
  EXPECT_THROW(in.ReadByte(), std::runtime_error);
}
