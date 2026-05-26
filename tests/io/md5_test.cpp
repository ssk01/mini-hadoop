#include <gtest/gtest.h>
#include "io/md5.h"

using namespace mini_hadoop;

TEST(MD5HashTest, ComputesCorrectHash) {
  std::string input = "hello";
  auto h = MD5Hash::Compute(input);
  // MD5("hello") = 5d41402abc4b2a76b9719d911017c592
  EXPECT_EQ(h.ToHexString(), "5d41402abc4b2a76b9719d911017c592");
}

TEST(MD5HashTest, EmptyInput) {
  auto h = MD5Hash::Compute(std::string_view(""));
  EXPECT_EQ(h.ToHexString(), "d41d8cd98f00b204e9800998ecf8427e");
}

TEST(MD5HashTest, WriteReadRoundTrip) {
  auto h1 = MD5Hash::Compute(std::string_view("test data"));

  OutputBuffer out;
  h1.Write(out);

  InputBuffer in;
  in.Reset(out.Data());
  MD5Hash h2;
  h2.ReadFields(in);

  EXPECT_EQ(h1.ToHexString(), h2.ToHexString());
  EXPECT_EQ(h1.CompareTo(h2), 0);
}

TEST(MD5HashTest, Compare) {
  auto h1 = MD5Hash::Compute(std::string_view("aaa"));
  auto h2 = MD5Hash::Compute(std::string_view("bbb"));
  EXPECT_NE(h1.CompareTo(h2), 0);
}
