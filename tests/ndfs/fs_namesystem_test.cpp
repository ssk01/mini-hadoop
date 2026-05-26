#include <gtest/gtest.h>
#include "ndfs/fs_namesystem.h"

using namespace mini_hadoop::ndfs;

TEST(FsNamesystemTest, StartFileAndGetBlocks) {
  FsNamesystem ns;

  DataNodeInfo dn;
  dn.name = "dn1";
  dn.host = "127.0.0.1";
  dn.port = 50010;
  dn.capacity = 1000000000;
  dn.remaining = 900000000;
  ns.RegisterDataNode(dn);

  auto result = ns.StartFile("/test.txt", false);
  EXPECT_NE(result.block.block_id, 0);
  EXPECT_FALSE(result.targets.empty());

  auto blocks = ns.GetBlocks("/test.txt");
  EXPECT_EQ(blocks.blocks.size(), 1);
}

TEST(FsNamesystemTest, MkdirsAndList) {
  FsNamesystem ns;
  EXPECT_TRUE(ns.Mkdirs("/a/b"));
  EXPECT_TRUE(ns.Mkdirs("/a/c"));

  auto list = ns.GetListing("/a");
  EXPECT_EQ(list.size(), 2);
}

TEST(FsNamesystemTest, Delete) {
  FsNamesystem ns;
  ns.StartFile("/del.txt", false);
  EXPECT_TRUE(ns.Delete("/del.txt"));

  auto blocks = ns.GetBlocks("/del.txt");
  EXPECT_EQ(blocks.blocks.size(), 0);
}

TEST(FsNamesystemTest, Heartbeat) {
  FsNamesystem ns;

  DataNodeInfo dn;
  dn.name = "dn1";
  dn.host = "127.0.0.1";
  dn.port = 50010;
  ns.RegisterDataNode(dn);
  ns.Heartbeat("dn1", "127.0.0.1", 50010, 1000, 500);

  auto report = ns.GetDataNodeReport();
  EXPECT_EQ(report.size(), 1);
  EXPECT_EQ(report[0].capacity, 1000);
  EXPECT_EQ(report[0].remaining, 500);
}

TEST(FsNamesystemTest, BlockReport) {
  FsNamesystem ns;

  DataNodeInfo dn;
  dn.name = "dn1";
  dn.host = "127.0.0.1";
  ns.RegisterDataNode(dn);

  std::vector<Block> blocks;
  Block b;
  b.block_id = 42;
  b.num_bytes = 100;
  blocks.push_back(b);

  ns.BlockReport("dn1", blocks);
  EXPECT_EQ(ns.AliveDataNodes(), 1);
}
