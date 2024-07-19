#include "execution/executors/topn_executor.h"
#include <atomic>
#include <queue>
#include <vector>
#include "common/rid.h"
#include "storage/table/tuple.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  this->plan_ = plan;
  this->child_executor_ = std::move(child_executor);
}

void TopNExecutor::Init() {
  // throw NotImplementedException("TopNExecutor is not implemented");
  child_executor_->Init();
  std::priority_queue<Tuple, std::vector<Tuple>, HeapComparator> heap(
      HeapComparator(&this->GetOutputSchema(), plan_->GetOrderBy()));
  Tuple tuple{};
  RID rid{};
  while (child_executor_->Next(&tuple, &rid)) {
    heap.push(tuple);
    heap_size_++;
    if (heap_size_ > plan_->GetN()) {
      heap.pop();
      heap_size_--;
    }
  }
  while (!heap.empty()) {
    this->top_entries_.push(heap.top());
    heap.pop();
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (top_entries_.empty()) {
    return false;
  }
  *tuple = top_entries_.top();
  top_entries_.pop();
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return top_entries_.size(); }

}  // namespace bustub
