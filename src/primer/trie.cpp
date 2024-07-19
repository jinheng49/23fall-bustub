#include "primer/trie.h"
#include <cstddef>
#include <memory>
#include <string_view>
#include <utility>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // throw NotImplementedException("Trie::Get is not implemented.");

  if (key.empty()) {
    const auto *tnwv = dynamic_cast<const bustub::TrieNodeWithValue<T> *>(root_.get());
    return tnwv == nullptr ? nullptr : tnwv->value_.get();
  }

  if (this->root_ == nullptr) {
    return nullptr;
  }
  std::shared_ptr<const bustub::TrieNode> cur = this->root_;
  for (char i : key) {
    auto it = cur->children_.find(i);
    if (it == cur->children_.end()) {
      return nullptr;
    }
    cur = it->second;
  }
  if (cur->is_value_node_) {
    const auto *tnwv = dynamic_cast<const bustub::TrieNodeWithValue<T> *>(cur.get());
    return tnwv == nullptr ? nullptr : tnwv->value_.get();
  }
  return nullptr;

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
}

template <class T>
void PutCycle(const std::shared_ptr<bustub::TrieNode> &new_root, std::string_view key, T val) {
  bool find = false;
  for (auto &pair : new_root->children_) {
    if (key.at(0) == pair.first) {
      find = true;
      if (key.size() > 1) {
        std::shared_ptr<bustub::TrieNode> ptr = pair.second->Clone();
        PutCycle<T>(ptr, key.substr(1, key.size() - 1), std::move(val));
        pair.second = std::shared_ptr<const TrieNode>(ptr);
      } else {
        std::shared_ptr<T> val_p = std::make_shared<T>(std::move(val));
        TrieNodeWithValue node_with_val(pair.second->children_, val_p);
        pair.second = std::make_shared<const TrieNodeWithValue<T>>(node_with_val);
      }
      return;
    }
  }

  if (!find) {
    char c = key.at(0);
    if (key.size() == 1) {
      std::shared_ptr<T> val_p = std::make_shared<T>(std::move(val));
      new_root->children_.insert({c, std::make_shared<const TrieNodeWithValue<T>>(val_p)});
    } else {
      auto ptr = std::make_shared<TrieNode>();
      PutCycle<T>(ptr, key.substr(1, key.size() - 1), std::move(val));
      new_root->children_.insert({c, std::move(ptr)});
    }
  }
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // throw NotImplementedException("Trie::Put is not implemented.");

  // ! 在根节点插入
  if (key.empty()) {
    std::shared_ptr<T> val_p = std::make_shared<T>(std::move(value));
    std::shared_ptr<bustub::TrieNodeWithValue<T>> new_root = nullptr;

    // 如果根节点无子节点
    if (root_->children_.empty()) {
      // 直接修改根节点
      new_root = std::make_unique<TrieNodeWithValue<T>>(std::move(val_p));
    } else {
      // 如果有，构造一个新节点，children指向root_的children
      new_root = std::make_unique<TrieNodeWithValue<T>>(root_->children_, std::move(val_p));
    }
    // 返回新的Trie
    return Trie(std::move(new_root));
  }

  // ! 拷贝根节点，如果为空，则新建一个空的TrieNode
  std::shared_ptr<bustub::TrieNode> new_root = nullptr;
  if (this->root_ == nullptr) {
    new_root = std::make_unique<TrieNode>();
  } else {
    new_root = root_->Clone();
  }

  // ! 递归插入
  PutCycle<T>(new_root, key, std::move(value));

  // ! 返回新的Trie
  return Trie(std::move(new_root));

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
}

auto RemoveCycle(const std::shared_ptr<bustub::TrieNode> &new_root, std::string_view key) -> bool {
  // 在new_root的children找key的第一个元素
  for (auto &pair : new_root->children_) {
    // 继续找
    if (key.at(0) != pair.first) {
      continue;
    }

    if (key.size() == 1) {
      // 是键结尾
      if (!pair.second->is_value_node_) {
        return false;
      }
      // 如果子节点为空，直接删除
      if (pair.second->children_.empty()) {
        new_root->children_.erase(pair.first);
      } else {
        // 否则转为tirenode
        pair.second = std::make_shared<const TrieNode>(pair.second->children_);
      }
      return true;
    }

    // 拷贝一份当前节点
    std::shared_ptr<bustub::TrieNode> ptr = pair.second->Clone();
    // 递归删除
    bool flag = RemoveCycle(ptr, key.substr(1, key.size() - 1));
    // 如果没有可删除的键
    if (!flag) {
      return false;
    }
    // 如果删除后当前节点无value且子节点为空，则删除
    if (ptr->children_.empty() && !ptr->is_value_node_) {
      new_root->children_.erase(pair.first);
    } else {
      // 否则将删除的子树覆盖原来的子树
      pair.second = std::shared_ptr<const TrieNode>(ptr);
    }
    return true;
  }
  return false;
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // throw NotImplementedException("Trie::Remove is not implemented.");

  if (this->root_ == nullptr) {
    return *this;
  }

  // 键为空
  if (key.empty()) {
    // 根节点有value
    if (root_->is_value_node_) {
      // 根节点无子节点
      if (root_->children_.empty()) {
        // 直接返回一个空的trie
        return {};
      }
      std::shared_ptr<bustub::TrieNode> new_root = std::make_shared<TrieNode>(root_->children_);
      return Trie(new_root);
    }
    // 根节点无value，直接返回
    return *this;
  }

  // 创建一个当前根节点的副本作为新的根节点
  std::shared_ptr<bustub::TrieNode> new_root = root_->Clone();

  // 删除
  bool flag = RemoveCycle(new_root, key);
  if (!flag) {
    return *this;
  }

  if (new_root->children_.empty() && !new_root->is_value_node_) {
    new_root = nullptr;
  }

  return Trie(std::move(new_root));

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
