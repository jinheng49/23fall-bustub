//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <utility>

#include "execution/executors/update_executor.h"
#include "storage/table/tuple.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  // throw NotImplementedException("UpdateExecutor is not implemented");
  this->child_executor_->Init();
  this->has_inserted_ = false;
}

auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (has_inserted_) {
    return false;
  }
  this->has_inserted_ = true;
  int count = 0;
  Tuple child_tuple{};
  RID child_rid{};
  auto table_info = this->exec_ctx_->GetCatalog()->GetTable(this->plan_->GetTableOid());
  auto indexes = this->exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
  while (this->child_executor_->Next(&child_tuple, &child_rid)) {
    count++;
    table_info->table_->UpdateTupleMeta(TupleMeta{0, true}, child_rid);
    std::vector<Value> new_values{};
    new_values.reserve(plan_->target_expressions_.size());
    for (const auto &expr : plan_->target_expressions_) {
      new_values.push_back(expr->Evaluate(&child_tuple, child_executor_->GetOutputSchema()));
    }
    auto update_tuple = Tuple{new_values, &table_info->schema_};
    std::optional<RID> new_rid_optional = table_info->table_->InsertTuple(TupleMeta{0, false}, update_tuple);
    RID new_rid;
    if (new_rid_optional.has_value()) {
      new_rid = new_rid_optional.value();
    }
    for (auto &index_info : indexes) {
      auto index = index_info->index_.get();
      auto key_attrs = index_info->index_->GetKeyAttrs();
      auto old_key = child_tuple.KeyFromTuple(table_info->schema_, *index->GetKeySchema(), key_attrs);
      auto new_key = update_tuple.KeyFromTuple(table_info->schema_, *index->GetKeySchema(), key_attrs);
      index->DeleteEntry(old_key, child_rid, this->exec_ctx_->GetTransaction());
      index->InsertEntry(new_key, new_rid, this->exec_ctx_->GetTransaction());
    }
  }
  std::vector<Value> result = {{TypeId::INTEGER, count}};
  *tuple = Tuple(result, &GetOutputSchema());
  return true;
}

}  // namespace bustub
