//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <optional>
#include <utility>

#include "common/exception.h"
#include "execution/execution_common.h"
#include "catalog/catalog.h"
#include "execution/executors/insert_executor.h"
#include "storage/table/tuple.h"


namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  child_executor_ = std::move(child_executor);
}

void InsertExecutor::Init() {
  // throw NotImplementedException("InsertExecutor is not implemented");
  this->child_executor_->Init();
  this->has_inserted_ = false;
  this->txn_ = exec_ctx_->GetTransaction();
  this->txn_mgr_ = exec_ctx_->GetTransactionManager();
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (has_inserted_) {
    return false;
  }
  this->has_inserted_ = true;
  int count = 0;
  auto table_info = this->exec_ctx_->GetCatalog()->GetTable(this->plan_->GetTableOid());
  auto schema = table_info->schema_;
  auto indexes = this->exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
  IndexInfo *primary_key_idx_info = indexes.empty()? nullptr : indexes[0];
  Tuple child_tuple{};
  while (this->child_executor_->Next(tuple, rid)) {
    count++;
    if (primary_key_idx_info == nullptr) {
      auto rid_optional = table_info->table_->InsertTuple(TupleMeta{txn_->GetTransactionTempTs(), false}, child_tuple,
                                                          exec_ctx_->GetLockManager(), txn_, plan_->GetTableOid());
      if(!rid_optional.has_value()){
        throw Exception("Invalid rid");
      }                                               
      txn_mgr_->UpdateUndoLink(*rid_optional, UndoLink());
      txn_->AppendWriteSet(table_info->oid_, *rid_optional);
    } else {
      InsertTuple(primary_key_idx_info, table_info, txn_mgr_, txn_, exec_ctx_->GetLockManager(), child_tuple,
                  &child_executor_->GetOutputSchema());
    }
  }
  std::vector<Value> result = {{TypeId::INTEGER, count}};
  *tuple = Tuple(result, &GetOutputSchema());
  return true;
}

}  // namespace bustub
