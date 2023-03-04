#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  index_ = 0;

  Tuple child_tuple{};
  RID rid;
  auto status = child_executor_->Next(&child_tuple, &rid);

  while (status) {
    tuples_.push_back(child_tuple);
    status = child_executor_->Next(&child_tuple, &rid);
  }

  auto orderbytypes = plan_->GetOrderBy();
  std::sort(tuples_.begin(), tuples_.end(), [&](Tuple &a, Tuple &b) {
    for (auto &orderbytype : orderbytypes) {
      Value key1 = orderbytype.second->Evaluate(&a, GetOutputSchema());
      Value key2 = orderbytype.second->Evaluate(&b, GetOutputSchema());
      if (key1.CompareEquals(key2) == CmpBool::CmpTrue) {
        continue;
      }
      return orderbytype.first == OrderByType::DESC ? key1.CompareLessThan(key2) == CmpBool::CmpFalse
                                                    : key1.CompareLessThan(key2) == CmpBool::CmpTrue;
    }

    return true;
  });
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (index_ == tuples_.size()) {
    return false;
  }

  *tuple = tuples_[index_];
  ++index_;
  return true;
}

}  // namespace bustub
