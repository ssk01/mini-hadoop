#include <iostream>
#include <memory>
#include <spdlog/spdlog.h>
#include <toml++/toml.hpp>

#include "common/error.h"
#include "ndfs/name_node.h"
#include "ndfs/data_node.h"
#include "ndfs/dfs_client.h"
#include "mapred/job_tracker.h"
#include "mapred/examples/wordcount.h"

using namespace mini_hadoop;

int main(int argc, const char* argv[]) {
  spdlog::set_level(spdlog::level::info);

  if (argc < 2) {
    std::cerr << "Usage: mini-hadoop <command> [args...]\n";
    std::cerr << "Commands:\n";
    std::cerr << "  version           Show version\n";
    std::cerr << "  namenode [port]   Start NameNode (default port 9000)\n";
    std::cerr << "  datanode [nn_host] [nn_port] [bs_port]\n";
    std::cerr << "  dfs <op> [args]   NDFS client operations\n";
    std::cerr << "  wordcount <input> <output>  Run WordCount\n";
    return 1;
  }

  std::string cmd = argv[1];

  if (cmd == "version") {
    std::cout << "mini-hadoop version " << MINI_HADOOP_VERSION << "\n";
    return 0;
  }

  if (cmd == "namenode") {
    int port = (argc > 2) ? std::stoi(argv[2]) : 9000;
    ndfs::NameNode nn(port);
    if (!nn.Start()) return 1;
    spdlog::info("NameNode running on port {}, press Ctrl+C to stop", port);
    while (true) std::this_thread::sleep_for(std::chrono::seconds(1));
    return 0;
  }

  if (cmd == "datanode") {
    std::string nn_host = (argc > 2) ? argv[2] : "127.0.0.1";
    int nn_port = (argc > 3) ? std::stoi(argv[3]) : 9000;
    int bs_port = (argc > 4) ? std::stoi(argv[4]) : 9010;
    std::string data_dir = "/tmp/mini-hadoop-datanode";

    ndfs::DataNode dn(data_dir, nn_host, nn_port, bs_port);
    if (!dn.Start()) return 1;
    spdlog::info("DataNode running, press Ctrl+C to stop");
    while (true) std::this_thread::sleep_for(std::chrono::seconds(1));
    return 0;
  }

  if (cmd == "dfs") {
    if (argc < 3) {
      std::cerr << "Usage: mini-hadoop dfs <put|get|ls|rm|mkdir> [args]\n";
      return 1;
    }
    std::string op = argv[2];
    ndfs::DfsClient client("127.0.0.1", 9000);
    if (!client.Connect().ok()) {
      std::cerr << "Cannot connect to NameNode\n";
      return 1;
    }

    if (op == "put" && argc >= 5) {
      std::string local = argv[3];
      std::string remote = argv[4];
      std::ifstream f(local, std::ios::binary | std::ios::ate);
      if (!f) { std::cerr << "Cannot open: " << local << "\n"; return 1; }
      auto sz = f.tellg();
      f.seekg(0);
      std::vector<uint8_t> data(static_cast<size_t>(sz));
      f.read(reinterpret_cast<char*>(data.data()), sz);
      f.close();

      auto st = client.WriteFile(remote, data.data(), data.size(), true);
      if (!st.ok()) { std::cerr << "Write failed: " << st.message() << "\n"; return 1; }
      std::cout << "Written " << data.size() << " bytes to " << remote << "\n";
    } else if (op == "get" && argc >= 5) {
      std::string remote = argv[3];
      std::string local = argv[4];
      std::vector<uint8_t> data;
      auto st = client.ReadFile(remote, data);
      if (!st.ok()) { std::cerr << "Read failed: " << st.message() << "\n"; return 1; }

      std::ofstream f(local, std::ios::binary);
      f.write(reinterpret_cast<char*>(data.data()), data.size());
      f.close();
      std::cout << "Read " << data.size() << " bytes from " << remote << "\n";
    } else if (op == "ls") {
      std::string path = (argc > 3) ? argv[3] : "/";
      auto list = client.List(path);
      for (const auto& item : list) std::cout << item << "\n";
    } else if (op == "rm" && argc > 3) {
      auto st = client.Delete(argv[3]);
      if (!st.ok()) std::cerr << "Delete failed: " << st.message() << "\n";
      else std::cout << "Deleted\n";
    } else if (op == "mkdir" && argc > 3) {
      auto st = client.Mkdirs(argv[3]);
      if (!st.ok()) std::cerr << "Mkdir failed: " << st.message() << "\n";
      else std::cout << "Created\n";
    }

    client.Disconnect();
    return 0;
  }

  if (cmd == "wordcount") {
    if (argc < 4) {
      std::cerr << "Usage: mini-hadoop wordcount <input> <output>\n";
      return 1;
    }

    mapred::JobConfig config;
    config.input_path = argv[2];
    config.output_path = argv[3];
    config.num_reduce_tasks = 1;

    mapred::JobTracker tracker;
    tracker.SubmitJob(config,
        []() { return std::make_unique<examples::WordCountMapper>(); },
        []() { return std::make_unique<examples::WordCountReducer>(); });
    tracker.RunJob();

    std::cout << "WordCount done, output: " << config.output_path << "\n";
    return 0;
  }

  std::cerr << "Unknown command: " << cmd << "\n";
  return 1;
}
