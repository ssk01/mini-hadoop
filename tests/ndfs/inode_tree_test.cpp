#include <gtest/gtest.h>
#include "ndfs/inode_tree.h"

using namespace mini_hadoop::ndfs;

TEST(InodeTreeTest, RootDirectory) {
  InodeTree tree;
  auto* root = tree.GetNode("/");
  ASSERT_NE(root, nullptr);
  EXPECT_EQ(root->type, INode::kDirectory);
}

TEST(InodeTreeTest, MkdirsSingle) {
  InodeTree tree;
  auto* dir = tree.Mkdirs("/foo");
  ASSERT_NE(dir, nullptr);
  EXPECT_EQ(dir->type, INode::kDirectory);
  EXPECT_EQ(dir->name, "foo");
}

TEST(InodeTreeTest, MkdirsNested) {
  InodeTree tree;
  auto* dir = tree.Mkdirs("/a/b/c");
  ASSERT_NE(dir, nullptr);
  EXPECT_EQ(dir->name, "c");

  auto* a = tree.GetNode("/a");
  ASSERT_NE(a, nullptr);
  auto* b = tree.GetNode("/a/b");
  ASSERT_NE(b, nullptr);
}

TEST(InodeTreeTest, CreateAndGetFile) {
  InodeTree tree;
  tree.Mkdirs("/data");
  auto* file = tree.CreateFile("/data/test.txt");
  ASSERT_NE(file, nullptr);
  EXPECT_EQ(file->type, INode::kFile);
  EXPECT_EQ(file->name, "test.txt");

  auto* found = tree.GetNode("/data/test.txt");
  ASSERT_NE(found, nullptr);
  EXPECT_EQ(found->type, INode::kFile);
}

TEST(InodeTreeTest, DeleteFile) {
  InodeTree tree;
  tree.CreateFile("/a.txt");
  ASSERT_NE(tree.GetNode("/a.txt"), nullptr);

  EXPECT_TRUE(tree.Delete("/a.txt"));
  EXPECT_EQ(tree.GetNode("/a.txt"), nullptr);
}

TEST(InodeTreeTest, ListDir) {
  InodeTree tree;
  tree.CreateFile("/f1");
  tree.CreateFile("/f2");
  tree.Mkdirs("/d1");

  auto list = tree.ListDir("/");
  EXPECT_EQ(list.size(), 3);
}

TEST(InodeTreeTest, GetNonExistent) {
  InodeTree tree;
  EXPECT_EQ(tree.GetNode("/no/such/path"), nullptr);
}
