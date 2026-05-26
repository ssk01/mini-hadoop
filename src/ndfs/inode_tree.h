#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <ctime>

namespace mini_hadoop {
namespace ndfs {

struct INode {
  enum Type { kDirectory, kFile };

  Type type;
  std::string name;
  int64_t modification_time = 0;
  std::vector<int64_t> blocks;  // block IDs for files
  std::vector<std::unique_ptr<INode>> children;  // for directories
  INode* parent = nullptr;

  INode(Type t, std::string n) : type(t), name(std::move(n)) {
    modification_time = std::time(nullptr);
  }
};

class InodeTree {
 public:
  InodeTree() {
    root_ = std::make_unique<INode>(INode::kDirectory, "");
  }

  INode* Root() { return root_.get(); }
  const INode* Root() const { return root_.get(); }

  INode* GetNode(const std::string& path) {
    auto parts = SplitPath(path);
    INode* cur = root_.get();
    if (parts.empty()) return cur;

    for (const auto& part : parts) {
      cur = FindChild(cur, part);
      if (!cur) return nullptr;
    }
    return cur;
  }

  INode* Mkdirs(const std::string& path) {
    auto parts = SplitPath(path);
    INode* cur = root_.get();
    for (const auto& part : parts) {
      auto* child = FindChild(cur, part);
      if (!child) {
        auto new_node = std::make_unique<INode>(INode::kDirectory, part);
        new_node->parent = cur;
        cur->children.push_back(std::move(new_node));
        cur = cur->children.back().get();
      } else if (child->type == INode::kDirectory) {
        cur = child;
      } else {
        return nullptr;  // path exists as file
      }
    }
    return cur;
  }

  INode* CreateFile(const std::string& path) {
    auto parts = SplitPath(path);
    if (parts.empty()) return nullptr;

    std::string filename = parts.back();
    parts.pop_back();

    INode* parent = root_.get();
    for (const auto& part : parts) {
      parent = FindChild(parent, part);
      if (!parent || parent->type != INode::kDirectory) return nullptr;
    }

    if (FindChild(parent, filename)) return nullptr;  // already exists

    auto file = std::make_unique<INode>(INode::kFile, filename);
    file->parent = parent;
    parent->children.push_back(std::move(file));
    return parent->children.back().get();
  }

  bool Delete(const std::string& path) {
    auto parts = SplitPath(path);
    if (parts.empty()) return false;

    INode* parent = root_.get();
    for (size_t i = 0; i < parts.size() - 1; i++) {
      parent = FindChild(parent, parts[i]);
      if (!parent) return false;
    }
    return RemoveChild(parent, parts.back());
  }

  std::vector<INode*> ListDir(const std::string& path) {
    auto* node = GetNode(path);
    if (!node || node->type != INode::kDirectory) return {};
    std::vector<INode*> result;
    for (auto& child : node->children) result.push_back(child.get());
    return result;
  }

 private:
  static std::vector<std::string> SplitPath(const std::string& path) {
    std::vector<std::string> parts;
    if (path.empty() || path == "/") return parts;
    size_t start = (path[0] == '/') ? 1 : 0;
    std::string current;
    for (size_t i = start; i <= path.size(); i++) {
      if (i == path.size() || path[i] == '/') {
        if (!current.empty()) {
          parts.push_back(current);
          current.clear();
        }
      } else {
        current += path[i];
      }
    }
    return parts;
  }

  static INode* FindChild(INode* parent, const std::string& name) {
    for (auto& child : parent->children) {
      if (child->name == name) return child.get();
    }
    return nullptr;
  }

  static bool RemoveChild(INode* parent, const std::string& name) {
    for (auto it = parent->children.begin(); it != parent->children.end(); ++it) {
      if ((*it)->name == name) {
        parent->children.erase(it);
        return true;
      }
    }
    return false;
  }

  std::unique_ptr<INode> root_;
};

}  // namespace ndfs
}  // namespace mini_hadoop
