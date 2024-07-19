//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <cstddef>
#include <new>
#include "common/config.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> access_lock(latch_);
  bool find = false;
  std::vector<size_t> inf_k_d_frame_id;
  size_t max_k_d = 0;

  frame_id_t max_k_d_id = replacer_size_;
  for (auto &node : node_store_) {
    if (!node.second.GetEvictable()) {
      continue;
    }
    size_t tmp = node.second.GetKDistance(current_timestamp_);

    if (tmp > max_k_d) {
      find = true;
      max_k_d = tmp;
      max_k_d_id = node.first;
    }

    if (tmp == UINT32_MAX) {
      inf_k_d_frame_id.push_back(node.first);
    }
  }

  *frame_id = max_k_d_id;
  size_t min_k_d = UINT32_MAX;

  for (size_t &i : inf_k_d_frame_id) {
    if (node_store_[i].GetBackAccess() < min_k_d) {
      min_k_d = node_store_[i].GetBackAccess();
      *frame_id = i;
    }
  }

  if (find) {
    node_store_[*frame_id].RemoveHistory();
    node_store_[*frame_id].SetEvictable(false);
    curr_size_--;
    return true;
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> access_lock(latch_);

  if (static_cast<size_t>(frame_id) > replacer_size_) {
    throw Exception("LRUKReplacer::RecordAccess: frame_id is invalid");
  }

  if (node_store_.find(frame_id) == node_store_.end()) {
    if (node_store_.size() == replacer_size_) {
      return;
    }
    node_store_[frame_id] = LRUKNode(frame_id, k_);
  }

  node_store_[frame_id].AddHistory(current_timestamp_);
  current_timestamp_++;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> access_lock(latch_);
  if (static_cast<size_t>(frame_id) > replacer_size_) {
    throw Exception("LRUKReplacer::RecordAccess: frame_id is invalid");
  }

  if (!set_evictable && node_store_[frame_id].GetEvictable()) {
    curr_size_--;
  } else if (set_evictable && !node_store_[frame_id].GetEvictable()) {
    curr_size_++;
  }
  node_store_[frame_id].SetEvictable(set_evictable);
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> access_lock(latch_);
  if (node_store_.find(frame_id) == node_store_.end()) {
    return;
  }
  if (!node_store_[frame_id].GetEvictable()) {
    throw Exception("LRUKReplacer::Remove: frame_id can not be remove");
  }
  node_store_[frame_id].RemoveHistory();
  node_store_[frame_id].SetEvictable(false);
  node_store_.erase(frame_id);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
