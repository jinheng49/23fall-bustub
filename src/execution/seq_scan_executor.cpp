//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include <cstddef>
#include "storage/table/table_heap.h"

namespace bustub {

// SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx)
// {}
SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  // throw NotImplementedException("SeqScanExecutor is not implemented");
  table_heap_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get();
  auto iter = table_heap_->MakeIterator();
  rids_.clear();
  while (!iter.IsEnd()) {
    rids_.push_back(iter.GetRID());
    ++iter;
  }
  rid_iter_ = rids_.begin();
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  TupleMeta meta{};
  do {
    if (rid_iter_ == rids_.end()) {
      return false;
    }
    meta = table_heap_->GetTuple(*rid_iter_).first;
    if (!meta.is_deleted_) {
      *tuple = table_heap_->GetTuple(*rid_iter_).second;
      *rid = *rid_iter_;
    }
    ++rid_iter_;
  } while (meta.is_deleted_ ||
           (plan_->filter_predicate_ != nullptr &&
            !plan_->filter_predicate_
                 ->Evaluate(tuple, GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid())->schema_)
                 .GetAs<bool>()));
  return true;
}

}  // namespace bustub
