//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_directory_page.cpp
//
// Identification: src/storage/page/extendible_htable_directory_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_directory_page.h"
// #include <_types/_uint32_t.h>
#include <algorithm>
#include <cstdint>
#include <unordered_map>

#include "common/config.h"
#include "common/logger.h"

namespace bustub {

void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) {
  // throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  this->max_depth_ = max_depth;
  global_depth_ = 0;
  std::fill(local_depths_, local_depths_ + (1 << max_depth_), 0);
  std::fill(bucket_page_ids_, bucket_page_ids_ + (1 << max_depth_), INVALID_PAGE_ID);
}

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t {
  return hash & GetGlobalDepthMask();
}

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t {
  return this->bucket_page_ids_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  // throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  this->bucket_page_ids_[bucket_idx] = bucket_page_id;
}

auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t {
  return bucket_idx + (1 << (global_depth_ - 1));
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepthMask() const -> uint32_t {
  auto depth = global_depth_;
  return (1 << depth) - 1;
}

auto ExtendibleHTableDirectoryPage::GetLocalDepthMask(uint32_t bucket_idx) const -> uint32_t {
  if (bucket_idx >= static_cast<uint32_t>(1 << global_depth_)) {
    throw std::out_of_range("Bucket index out of range");
  }
  uint32_t depth = local_depths_[bucket_idx];
  return (1 << depth) - 1;
}
auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t { return global_depth_; }

auto ExtendibleHTableDirectoryPage::GetMaxDepth() const -> uint32_t { return max_depth_; }

void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
  // throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  if (global_depth_ >= max_depth_) {
    return;
  }
  for (int i = 0; i < 1 << global_depth_; i++) {
    bucket_page_ids_[(1 << global_depth_) + i] = bucket_page_ids_[i];
    local_depths_[(1 << global_depth_) + i] = local_depths_[i];
  }
  global_depth_++;
}

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() {
  // throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  if (global_depth_ <= 0) {
    return;
  }
  global_depth_--;
}

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool {
  if (global_depth_ == 0) {
    return false;
  }
  for (uint32_t i = 0; i < Size(); i++) {
    if (local_depths_[i] == global_depth_) {
      return false;
    }
  }
  return true;
}

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t { return 1 << global_depth_; }

auto ExtendibleHTableDirectoryPage::MaxSize() const -> uint32_t { return 1 << max_depth_; }

auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t {
  return local_depths_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  // throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  local_depths_[bucket_idx] = local_depth;
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) {
  // throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  if (local_depths_[bucket_idx] < global_depth_) {
    local_depths_[bucket_idx]++;
  }
}

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) {
  // throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  if (local_depths_[bucket_idx] > 0) {
    local_depths_[bucket_idx]--;
  }
}

}  // namespace bustub
