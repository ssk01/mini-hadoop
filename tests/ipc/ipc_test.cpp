#include <gtest/gtest.h>
#include <thread>
#include <chrono>

#include "ipc/server.h"
#include "ipc/client.h"
#include "io/writable_types.h"

using namespace mini_hadoop;

TEST(IpcTest, PingPong) {
  ipc::RpcServer server(18900, 2);

  server.RegisterHandler(1, [](InputBuffer& req, OutputBuffer& resp) {
    resp.WriteInt(42);
  });

  ASSERT_TRUE(server.Start());
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  ipc::RpcClient client;
  ASSERT_TRUE(client.Connect("127.0.0.1", 18900));

  OutputBuffer empty_req;  // no request params
  std::vector<uint8_t> resp_data;
  ASSERT_TRUE(client.CallRaw(1, {}, resp_data).ok());

  InputBuffer resp(resp_data);
  EXPECT_EQ(resp.ReadInt(), 42);

  client.Disconnect();
  server.Stop();
}

TEST(IpcTest, MultipleRequests) {
  ipc::RpcServer server(18901, 2);

  server.RegisterHandler(100, [](InputBuffer& req, OutputBuffer& resp) {
    int32_t a = req.ReadInt();
    int32_t b = req.ReadInt();
    resp.WriteInt(a + b);
  });

  ASSERT_TRUE(server.Start());
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  for (int i = 0; i < 20; i++) {
    ipc::RpcClient client;
    ASSERT_TRUE(client.Connect("127.0.0.1", 18901));

    OutputBuffer req;
    req.WriteInt(i);
    req.WriteInt(i * 10);

    std::vector<uint8_t> resp_data;
    ASSERT_TRUE(client.CallRaw(100, req.Data(), resp_data).ok());

    InputBuffer resp(resp_data);
    EXPECT_EQ(resp.ReadInt(), i + i * 10);

    client.Disconnect();
  }

  server.Stop();
}

TEST(IpcTest, WritableRoundTrip) {
  ipc::RpcServer server(18902, 2);

  server.RegisterHandler(5, [](InputBuffer& req, OutputBuffer& resp) {
    int32_t v = req.ReadInt();
    resp.WriteLong(static_cast<int64_t>(v) * 100);
  });

  ASSERT_TRUE(server.Start());
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  ipc::RpcClient client;
  ASSERT_TRUE(client.Connect("127.0.0.1", 18902));

  OutputBuffer req_buf;
  req_buf.WriteInt(42);

  std::vector<uint8_t> resp;
  ASSERT_TRUE(client.CallRaw(5, req_buf.Data(), resp).ok());

  InputBuffer in(resp);
  EXPECT_EQ(in.ReadLong(), 4200);

  client.Disconnect();
  server.Stop();
}
