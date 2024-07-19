//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include <algorithm>
#include <cstdint>
#include <utility>
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "fmt/core.h"
#include "storage/table/tuple.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx) {
  this->plan_ = plan;
  this->left_executor_ = std::move(left_executor);
  this->right_executor_ = std::move(right_executor);
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER))
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  // throw NotImplementedException("NestedLoopJoinExecutor is not implemented");
  left_executor_->Init();
  right_executor_->Init();
  left_bool_ = left_executor_->Next(&left_tuple_, &left_rid_);
  has_done_ = false;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple right_tuple{};
  RID right_rid{};
  auto predicate = plan_->predicate_;
  if (plan_->GetJoinType() == JoinType::LEFT) {
    while (left_bool_) {
      while (right_executor_->Next(&right_tuple, &right_rid)) {
        auto join_result = predicate->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple,
                                                   right_executor_->GetOutputSchema());
        if (join_result.GetAs<bool>()) {
          std::vector<Value> values;
          for (uint32_t i = 0; i < this->left_executor_->GetOutputSchema().GetColumnCount(); i++) {
            values.emplace_back(left_tuple_.GetValue(&this->left_executor_->GetOutputSchema(), i));
          }
          for (uint32_t i = 0; i < this->right_executor_->GetOutputSchema().GetColumnCount(); i++) {
            values.emplace_back(right_tuple.GetValue(&this->right_executor_->GetOutputSchema(), i));
          }
          *tuple = Tuple{values, &GetOutputSchema()};
          has_done_ = true;
          return true;
        }
      }
      if (!has_done_) {
        std::vector<Value> values;
        for (uint32_t i = 0; i < this->left_executor_->GetOutputSchema().GetColumnCount(); i++) {
          values.emplace_back(left_tuple_.GetValue(&this->left_executor_->GetOutputSchema(), i));
        }
        for (uint32_t i = 0; i < this->right_executor_->GetOutputSchema().GetColumnCount(); i++) {
          values.emplace_back(
              ValueFactory::GetNullValueByType(this->right_executor_->GetOutputSchema().GetColumn(i).GetType()));
        }
        *tuple = Tuple{values, &GetOutputSchema()};
        has_done_ = true;
        return true;
      }
      left_bool_ = left_executor_->Next(&this->left_tuple_, &this->left_rid_);
      right_executor_->Init();
      has_done_ = false;
    }
    return false;
  }
  if (plan_->GetJoinType() == JoinType::INNER) {
    while (left_bool_) {
      while (right_executor_->Next(&right_tuple, &right_rid)) {
        auto join_result = predicate->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple,
                                                   right_executor_->GetOutputSchema());
        if (join_result.GetAs<bool>()) {
          std::vector<Value> values;
          for (uint32_t i = 0; i < this->left_executor_->GetOutputSchema().GetColumnCount(); i++) {
            values.emplace_back(left_tuple_.GetValue(&this->left_executor_->GetOutputSchema(), i));
          }
          for (uint32_t i = 0; i < this->right_executor_->GetOutputSchema().GetColumnCount(); i++) {
            values.emplace_back(right_tuple.GetValue(&this->right_executor_->GetOutputSchema(), i));
          }
          *tuple = Tuple{values, &GetOutputSchema()};
          return true;
        }
      }
      left_bool_ = left_executor_->Next(&this->left_tuple_, &this->left_rid_);
      right_executor_->Init();
    }
    return false;
  }
  return false;
}

}  // namespace bustub
