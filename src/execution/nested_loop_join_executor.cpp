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
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  index_ = 0;
  std::vector<Tuple> left_tuples{};
  std::vector<Tuple> right_tuples{};
  Tuple child_tuple{};
  RID rid;
  auto status = left_executor_->Next(&child_tuple, &rid);
  while (status) {
    left_tuples.push_back(child_tuple);
    status = left_executor_->Next(&child_tuple, &rid);
  }
  status = right_executor_->Next(&child_tuple, &rid);
  while (status) {
    right_tuples.push_back(child_tuple);
    status = right_executor_->Next(&child_tuple, &rid);
  }
  for (auto &left_tuple : left_tuples) {
    bool match_flag = false;
    for (auto &right_tuple : right_tuples) {
      Value is_match = plan_->Predicate().EvaluateJoin(&left_tuple, left_executor_->GetOutputSchema(), &right_tuple,
                                                       right_executor_->GetOutputSchema());
      if (is_match.ToString() == "true") {
        std::vector<Value> values{};
        match_flag = true;
        for (size_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); ++i) {
          values.emplace_back(left_tuple.GetValue(&left_executor_->GetOutputSchema(), i));
        }
        for (size_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); ++i) {
          values.emplace_back(right_tuple.GetValue(&right_executor_->GetOutputSchema(), i));
        }
        result_.emplace_back(Tuple(values, &GetOutputSchema()));
      }
    }
    if (plan_->GetJoinType() == JoinType::LEFT && !match_flag) {
      std::vector<Value> values{};
      for (size_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); ++i) {
        values.emplace_back(left_tuple.GetValue(&left_executor_->GetOutputSchema(), i));
      }
      for (size_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); ++i) {
        values.emplace_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
      }
      result_.emplace_back(Tuple(values, &GetOutputSchema()));
    }
  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (index_ == result_.size()) {
    return false;
  }

  *tuple = result_[index_];
  ++index_;

  return true;
}

}  // namespace bustub
