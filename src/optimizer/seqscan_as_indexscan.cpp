#include <filesystem>
#include <vector>
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule
  std::vector<bustub::AbstractPlanNodeRef> optimized_children;
  for (const auto &child : plan->GetChildren()) {
    optimized_children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(optimized_children));
  if (optimized_plan->GetType() == PlanType::SeqScan) {
    const auto &seq_plan = dynamic_cast<const bustub::SeqScanPlanNode &>(*optimized_plan);
    auto predicate = seq_plan.filter_predicate_;
    if (predicate != nullptr) {
      auto table_name = seq_plan.table_name_;
      auto table_idx = catalog_.GetTableIndexes(table_name);
      auto logic_expr = std::dynamic_pointer_cast<LogicExpression>(predicate);
      if (!table_idx.empty() && !logic_expr) {
        auto equal_expr = std::dynamic_pointer_cast<ComparisonExpression>(predicate);
        if (equal_expr) {
          auto com_type = equal_expr->comp_type_;
          if (com_type == ComparisonType::Equal) {
            auto table_oid = seq_plan.table_oid_;
            auto column_expr = dynamic_cast<const ColumnValueExpression &>(*equal_expr->GetChildAt(0));
            auto column_index = column_expr.GetColIdx();
            auto col_name = this->catalog_.GetTable(table_oid)->schema_.GetColumn(column_index).GetName();
            for (auto *index : table_idx) {
              const auto &columns = index->index_->GetKeyAttrs();
              std::vector<uint32_t> column_ids;
              column_ids.push_back(column_index);
              if (columns == column_ids) {
                auto pred_key = std::dynamic_pointer_cast<ConstantValueExpression>(equal_expr->GetChildAt(1));
                ConstantValueExpression *raw_pred_key = pred_key ? pred_key.get() : nullptr;
                return std::make_shared<IndexScanPlanNode>(seq_plan.output_schema_, table_oid, index->index_oid_,
                                                           predicate, raw_pred_key);
              }
            }
          }
        }
      }
    }
  }
  return optimized_plan;
}

}  // namespace bustub
