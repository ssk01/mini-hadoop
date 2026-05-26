#include <gtest/gtest.h>
#include <cstdio>
#include <filesystem>
#include "ndfs/block_storage.h"

using namespace mini_hadoop::ndfs;

TEST(BlockStorageTest, StoreAndRead) {
  std::string dir = "/tmp/mini-hadoop-test-blocks";
  std::filesystem::remove_all(dir);
  BlockStorage storage(dir);

  Block b;
  b.block_id = 1;
  b.num_bytes = 100;

  std::vector<uint8_t> data(100, 0xAB);
  ASSERT_TRUE(storage.StoreBlock(b, data.data(), data.size()).ok());

  std::vector<uint8_t> readback;
  ASSERT_TRUE(storage.ReadBlock(b.block_id, readback).ok());
  EXPECT_EQ(readback, data);

  std::filesystem::remove_all(dir);
}

TEST(BlockStorageTest, BlockReport) {
  std::string dir = "/tmp/mini-hadoop-test-blocks2";
  std::filesystem::remove_all(dir);
  BlockStorage storage(dir);

  Block b1{1, 50, 0}, b2{2, 100, 0};
  std::vector<uint8_t> d(50, 0);

  ASSERT_TRUE(storage.StoreBlock(b1, d.data(), d.size()).ok());
  ASSERT_TRUE(storage.StoreBlock(b2, d.data(), 100).ok());

  auto report = storage.GetBlockReport();
  EXPECT_EQ(report.size(), 2);

  std::filesystem::remove_all(dir);
}

TEST(BlockStorageTest, Remove) {
  std::string dir = "/tmp/mini-hadoop-test-blocks3";
  std::filesystem::remove_all(dir);
  BlockStorage storage(dir);

  Block b{99, 10, 0};
  std::vector<uint8_t> d(10, 0xFF);
  ASSERT_TRUE(storage.StoreBlock(b, d.data(), d.size()).ok());
  ASSERT_TRUE(storage.RemoveBlock(99).ok());

  std::vector<uint8_t> readback;
  EXPECT_FALSE(storage.ReadBlock(99, readback).ok());

  std::filesystem::remove_all(dir);
}
