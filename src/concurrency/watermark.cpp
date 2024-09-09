#include "concurrency/watermark.h"
#include <exception>
#include <mutex>
#include <shared_mutex>
#include "common/exception.h"

namespace bustub {

auto Watermark::AddTxn(timestamp_t read_ts) -> void {
  if (read_ts < commit_ts_) {
    throw Exception("read ts < commit ts");
  }
  std::unique_lock<std::shared_mutex> lck(latch_);
  // TODO(fall2023): implement me!
  if (current_reads_.count(read_ts) != 0U) {
    ++current_reads_[read_ts];
  } else {
    if (current_reads_.empty()) {
      watermark_ = read_ts;
    }
    current_reads_[read_ts] = 1;
  }
  watermark_ = std::min(watermark_, read_ts);
}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
  // TODO(fall2023): implement me!
  std::unique_lock<std::shared_mutex> lck(latch_);
  if (current_reads_[read_ts] == 1) {
    current_reads_.erase(read_ts);
  } else {
    --current_reads_[read_ts];
  }
  if (current_reads_.empty()) {
    watermark_ = commit_ts_;
  } else {
    watermark_ = current_reads_.begin()->first;
  }
}

}  // namespace bustub
