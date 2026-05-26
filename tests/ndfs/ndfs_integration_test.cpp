#include <gtest/gtest.h>
#include <chrono>
#include <filesystem>
#include <thread>

#include "ndfs/name_node.h"
#include "ndfs/data_node.h"
#include "ndfs/dfs_client.h"

using namespace mini_hadoop::ndfs;

class NdfsIntegrationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::filesystem::remove_all("/tmp/mini-hadoop-ndfs-test");
    std::filesystem::create_directories("/tmp/mini-hadoop-ndfs-test/dn");
  }

  void TearDown() override {
    std::filesystem::remove_all("/tmp/mini-hadoop-ndfs-test");
  }
};

TEST_F(NdfsIntegrationTest, FullWriteReadRoundTrip) {
  NameNode nn(19000);
  ASSERT_TRUE(nn.Start());
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  DataNode dn("/tmp/mini-hadoop-ndfs-test/dn", "127.0.0.1", 19000, 19100);
  ASSERT_TRUE(dn.Start());
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  DfsClient client("127.0.0.1", 19000);
  ASSERT_TRUE(client.Connect().ok());

  std::string test_data = "Hello, mini-hadoop NDFS! This is a test file.";
  ASSERT_TRUE(client.WriteFile("/test/hello.txt",
        reinterpret_cast<const uint8_t*>(test_data.data()),
        test_data.size(), true).ok());

  std::vector<uint8_t> readback;
  ASSERT_TRUE(client.ReadFile("/test/hello.txt", readback).ok());

  std::string read_str(reinterpret_cast<char*>(readback.data()), readback.size());
  EXPECT_EQ(read_str, test_data);

  auto list = client.List("/test");
  EXPECT_EQ(list.size(), 1);
  EXPECT_EQ(list[0], "hello.txt");

  client.Disconnect();
  dn.Stop();
  nn.Stop();
}

TEST_F(NdfsIntegrationTest, MkdirsAndDelete) {
  NameNode nn(19002);
  ASSERT_TRUE(nn.Start());
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  DfsClient client("127.0.0.1", 19002);
  ASSERT_TRUE(client.Connect().ok());

  EXPECT_TRUE(client.Mkdirs("/a/b/c").ok());
  auto list = client.List("/a/b");
  EXPECT_EQ(list.size(), 1);

  client.Disconnect();
  nn.Stop();
}

TEST_F(NdfsIntegrationTest, PipelineReplication) {
  NameNode nn(19006);
  ASSERT_TRUE(nn.Start());
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Start 2 DataNodes
  DataNode dn1("/tmp/mini-hadoop-ndfs-test/dn_a", "127.0.0.1", 19006, 19106);
  DataNode dn2("/tmp/mini-hadoop-ndfs-test/dn_b", "127.0.0.1", 19006, 19107);
  ASSERT_TRUE(dn1.Start());
  ASSERT_TRUE(dn2.Start());
  std::this_thread::sleep_for(std::chrono::milliseconds(300));

  DfsClient client("127.0.0.1", 19006);
  ASSERT_TRUE(client.Connect().ok());

  std::string test_data = "replicated data via pipeline!";
  ASSERT_TRUE(client.WriteFile("/replicated.txt",
        reinterpret_cast<const uint8_t*>(test_data.data()),
        test_data.size(), true).ok());

  // Read back — should work via any DN
  std::vector<uint8_t> readback;
  ASSERT_TRUE(client.ReadFile("/replicated.txt", readback).ok());

  std::string read_str(reinterpret_cast<char*>(readback.data()), readback.size());
  EXPECT_EQ(read_str, test_data);

  // Kill one DN, read still works
  dn1.Stop();
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  std::vector<uint8_t> readback2;
  ASSERT_TRUE(client.ReadFile("/replicated.txt", readback2).ok());
  EXPECT_EQ(readback2.size(), test_data.size());

  client.Disconnect();
  dn2.Stop();
  nn.Stop();
}

TEST_F(NdfsIntegrationTest, PersistenceAcrossRestart) {
  std::string fsimage = "/tmp/mini-hadoop-ndfs-test/fsimage";
  std::filesystem::remove(fsimage);

  // First run: create files
  {
    NameNode nn(19008, fsimage);
    ASSERT_TRUE(nn.Start());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    DataNode dn("/tmp/mini-hadoop-ndfs-test/dn_p", "127.0.0.1", 19008, 19108);
    ASSERT_TRUE(dn.Start());
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    DfsClient client("127.0.0.1", 19008);
    ASSERT_TRUE(client.Connect().ok());

    ASSERT_TRUE(client.Mkdirs("/data").ok());

    std::string data = "persistent data";
    ASSERT_TRUE(client.WriteFile("/data/file.txt",
          reinterpret_cast<const uint8_t*>(data.data()), data.size()).ok());

    client.Disconnect();
    dn.Stop();
    nn.Stop();
  }

  // Verify fsimage file exists
  EXPECT_TRUE(std::filesystem::exists(fsimage));

  // Second run: restart NameNode, data should survive
  {
    NameNode nn(19008, fsimage);
    ASSERT_TRUE(nn.Start());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    DataNode dn("/tmp/mini-hadoop-ndfs-test/dn_p", "127.0.0.1", 19008, 19108);
    ASSERT_TRUE(dn.Start());
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    DfsClient client("127.0.0.1", 19008);
    ASSERT_TRUE(client.Connect().ok());

    auto list = client.List("/data");
    EXPECT_EQ(list.size(), 1);
    EXPECT_EQ(list[0], "file.txt");

    std::vector<uint8_t> readback;
    auto st = client.ReadFile("/data/file.txt", readback);
    if (st.ok()) {
      std::string read_str(reinterpret_cast<char*>(readback.data()), readback.size());
      EXPECT_EQ(read_str, "persistent data");
    }

    client.Disconnect();
    dn.Stop();
    nn.Stop();
  }
}

TEST_F(NdfsIntegrationTest, MultiBlockWriteRead) {
  NameNode nn(19004);
  ASSERT_TRUE(nn.Start());
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  DataNode dn("/tmp/mini-hadoop-ndfs-test/dn2", "127.0.0.1", 19004, 19104);
  ASSERT_TRUE(dn.Start());
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // Use small block size (64 bytes) to trigger 4-block split (64+64+64+8 = 200)
  DfsClient client("127.0.0.1", 19004, 64);
  ASSERT_TRUE(client.Connect().ok());

  std::vector<uint8_t> test_data(200);
  for (size_t i = 0; i < 200; i++) test_data[i] = static_cast<uint8_t>(i % 256);
  ASSERT_TRUE(client.WriteFile("/large.bin", test_data.data(), test_data.size(), true).ok());

  std::vector<uint8_t> readback;
  ASSERT_TRUE(client.ReadFile("/large.bin", readback).ok());
  EXPECT_EQ(readback, test_data);

  client.Disconnect();
  dn.Stop();
  nn.Stop();
}
