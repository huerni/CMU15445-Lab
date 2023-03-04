#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();
  index_ = 0;
  auto orderbytypes = plan_->GetOrderBy();
  auto cmp = [&](Tuple &a, Tuple &b) {
    for (auto &orderbytype : orderbytypes) {
      Value key1 = orderbytype.second->Evaluate(&a, GetOutputSchema());
      Value key2 = orderbytype.second->Evaluate(&b, GetOutputSchema());
      if (key1.CompareEquals(key2) == CmpBool::CmpTrue) {
        continue;
      }
      return orderbytype.first == OrderByType::DESC ? key1.CompareLessThan(key2) == CmpBool::CmpTrue
                                                    : key1.CompareLessThan(key2) == CmpBool::CmpFalse;
    }

    return true;
  };

  std::priority_queue<Tuple, std::vector<Tuple>, decltype(cmp)> que(cmp);

  Tuple child_tuple{};
  RID rid;
  auto status = child_executor_->Next(&child_tuple, &rid);
  while (status) {
    que.push(child_tuple);
    status = child_executor_->Next(&child_tuple, &rid);
  }
  int emit_size = plan_->GetN();
  while ((emit_size--) != 0 && !que.empty()) {
    result_.push_back(que.top());
    que.pop();
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (index_ == result_.size()) {
    return false;
  }

  *tuple = result_[index_];
  ++index_;
  return true;
}

}  // namespace bustub
