//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  this->plan_ = plan;
  this->child_executor_ = std::move(child_executor);
}

void AggregationExecutor::Init() {
  child_executor_->Init();
  auto agg_exprs = plan_->GetAggregates();
  auto agg_types = plan_->GetAggregateTypes();
  aht_ = std::make_unique<SimpleAggregationHashTable>(plan_->GetAggregates(), plan_->GetAggregateTypes());
  Tuple child_tuple{};
  RID rid{};
  while (child_executor_->Next(&child_tuple, &rid)) {
    auto agg_key = MakeAggregateKey(&child_tuple);
    auto agg_val = MakeAggregateValue(&child_tuple);
    aht_->InsertCombine(agg_key, agg_val);
  }
  aht_iterator_ = std::make_unique<SimpleAggregationHashTable::Iterator>(aht_->Begin());
  has_inserted_ = false;
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (aht_->Begin() != aht_->End()) {
    if (*aht_iterator_ == aht_->End()) {
      return false;
    }
    const AggregateKey &agg_key = aht_iterator_->Key();
    const AggregateValue &agg_val = aht_iterator_->Val();
    std::vector<Value> values{};
    values.reserve(agg_key.group_bys_.size() + agg_val.aggregates_.size());
    for (auto &group_values : agg_key.group_bys_) {
      values.emplace_back(group_values);
    }
    for (auto &agg_value : agg_val.aggregates_) {
      values.emplace_back(agg_value);
    }
    *tuple = {values, &GetOutputSchema()};
    ++*aht_iterator_;
    return true;
  }
  if (has_inserted_) {
    return false;
  }
  has_inserted_ = true;
  if (plan_->GetGroupBys().empty()) {
    std::vector<Value> values;
    Tuple tuple_buffer{};
    for (auto &agg_value : aht_->GenerateInitialAggregateValue().aggregates_) {
      values.emplace_back(agg_value);
    }
    *tuple = {values, &GetOutputSchema()};
    return true;
  }
  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
