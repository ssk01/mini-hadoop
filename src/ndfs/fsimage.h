#pragma once

#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <spdlog/spdlog.h>

#include "ndfs/inode_tree.h"

namespace mini_hadoop {
namespace ndfs {

class FsImage {
 public:
  static void Save(InodeTree& tree, const std::string& path) {
    std::string tmp = path + ".tmp";
    std::ofstream f(tmp);
    if (!f) {
      spdlog::error("FsImage: cannot write {}", tmp);
      return;
    }

    f << "# mini-hadoop FsImage v1\n";
    SerializeNode(f, tree.Root(), "");

    f.close();
    std::rename(tmp.c_str(), path.c_str());
    spdlog::info("FsImage saved to {} ({} bytes)", path, std::filesystem::file_size(path));
  }

  static void Load(InodeTree& tree, const std::string& path) {
    std::ifstream f(path);
    if (!f) {
      spdlog::info("FsImage: no existing image at {}", path);
      return;
    }

    std::string line;
    std::getline(f, line);  // skip header

    while (std::getline(f, line)) {
      if (line.empty() || line[0] == '#') continue;

      std::istringstream iss(line);
      std::string type;
      iss >> type;

      if (type == "DIR") {
        std::string dirpath;
        iss >> dirpath;
        if (!dirpath.empty() && dirpath != "/") tree.Mkdirs(dirpath);
      } else if (type == "FILE") {
        std::string filepath, blocks_token;
        iss >> filepath >> blocks_token;

        // Create parent dirs and file
        auto parent = GetParentDir(filepath);
        if (parent != "/") tree.Mkdirs(parent);
        auto* node = tree.CreateFile(filepath);
        if (!node) continue;

        // Parse block IDs: blocks=123,456,789
        if (blocks_token.rfind("blocks=", 0) == 0) {
          auto blk_str = blocks_token.substr(7);
          std::istringstream biss(blk_str);
          std::string bid;
          while (std::getline(biss, bid, ',')) {
            if (!bid.empty()) node->blocks.push_back(std::stoll(bid));
          }
        }
      }
    }
    f.close();
    spdlog::info("FsImage loaded from {} ({} bytes)", path, std::filesystem::file_size(path));
  }

 private:
  static void SerializeNode(std::ofstream& f, const INode* node, const std::string& prefix) {
    for (const auto& child : node->children) {
      std::string full_path = prefix + "/" + child->name;
      if (child->type == INode::kDirectory) {
        f << "DIR " << full_path << "\n";
        SerializeNode(f, child.get(), full_path);
      } else {
        f << "FILE " << full_path << " blocks=";
        for (size_t i = 0; i < child->blocks.size(); i++) {
          if (i > 0) f << ",";
          f << child->blocks[i];
        }
        f << "\n";
      }
    }
  }

  static std::string GetParentDir(const std::string& path) {
    auto pos = path.rfind('/');
    if (pos == std::string::npos || pos == 0) return "/";
    return path.substr(0, pos);
  }
};

}  // namespace ndfs
}  // namespace mini_hadoop
