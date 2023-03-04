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

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(SimpleAggregationHashTable(plan_->GetAggregates(), plan_->GetAggregateTypes())),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_->Init();
  flag_ = true;
  Tuple child_tuple{};
  RID rid;
  auto status = child_->Next(&child_tuple, &rid);
  while (status) {
    AggregateKey keys = MakeAggregateKey(&child_tuple);
    AggregateValue values = MakeAggregateValue(&child_tuple);
    aht_.InsertCombine(keys, values);
    status = child_->Next(&child_tuple, &rid);
  }

  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  std::vector<Value> values{};
  if (aht_.Begin() == aht_.End() && flag_) {
    flag_ = false;
    if (GetOutputSchema().GetColumnCount() == aht_.GenerateInitialAggregateValue().aggregates_.size()) {
      for (auto &value : aht_.GenerateInitialAggregateValue().aggregates_) {
        values.push_back(value);
      }
    } else {
      return false;
    }

    *tuple = Tuple(values, &GetOutputSchema());
    return true;
  }

  if (aht_iterator_ == aht_.End() && !flag_) {
    return false;
  }

  flag_ = false;
  values.reserve(GetOutputSchema().GetColumnCount());
  auto keys = aht_iterator_.Key();
  for (auto &key : keys.group_bys_) {
    values.push_back(key);
  }

  auto aggvalues = aht_iterator_.Val();
  for (auto &value : aggvalues.aggregates_) {
    values.push_back(value);
  }

  if (values.empty()) {
    for (auto &value : aht_.GenerateInitialAggregateValue().aggregates_) {
      values.push_back(value);
    }
  }

  *tuple = Tuple(values, &GetOutputSchema());
  ++aht_iterator_;

  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
