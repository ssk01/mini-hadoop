#include <gtest/gtest.h>

#include "common/error.h"

TEST(StatusTest, OkStatus) {
  auto s = mini_hadoop::Status::OK();
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(s.message(), "");
}

TEST(StatusTest, ErrorStatuses) {
  auto s = mini_hadoop::Status::IOError("disk full");
  EXPECT_FALSE(s.ok());
  EXPECT_EQ(s.message(), "disk full");

  s = mini_hadoop::Status::Corrupt("checksum mismatch");
  EXPECT_FALSE(s.ok());
  EXPECT_EQ(s.message(), "checksum mismatch");

  s = mini_hadoop::Status::NotFound("file not found");
  EXPECT_FALSE(s.ok());
  EXPECT_EQ(s.message(), "file not found");

  s = mini_hadoop::Status::InvalidArg("bad argument");
  EXPECT_FALSE(s.ok());
  EXPECT_EQ(s.message(), "bad argument");
}
