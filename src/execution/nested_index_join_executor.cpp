//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan)
    , child_executor_(std::move(child_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() { 
    child_executor_->Init();
    index_ = 0;
    Tuple child_tuple {};
    RID rid;
    IndexInfo *indexinfo = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
    TableInfo * tableinfo = exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid());
    // 对于从子表中提出的外部元组，向catalog索引中找到内部表元组。
    // 使用key predicate构造索引探测键
    auto statue = child_executor_->Next(&child_tuple, &rid);
    while(statue) {
      Tuple key {};
      std::vector<Value> key_values {};
      for(size_t i = 0; i<indexinfo->key_schema_.GetColumnCount(); ++i) {
        key_values.push_back(plan_->KeyPredicate()->Evaluate(&child_tuple, child_executor_->GetOutputSchema()));
      }

      std::vector<RID> index_result;
      indexinfo->index_->ScanKey(Tuple(key_values, &indexinfo->key_schema_), &index_result, exec_ctx_->GetTransaction());
      std::vector<Value> values;
      for(size_t i = 0; i<child_executor_->GetOutputSchema().GetColumnCount(); ++i) {
        values.push_back(child_tuple.GetValue(&child_executor_->GetOutputSchema(), i));
      }
      for(auto& r : index_result) {
        Tuple right_tuple {};
        std::vector<Value> all_values;
        all_values.assign(values.begin(), values.end());
        tableinfo->table_->GetTuple(r, &right_tuple, exec_ctx_->GetTransaction());
        for(size_t i = 0; i<plan_->InnerTableSchema().GetColumnCount(); ++i) {
          all_values.push_back(right_tuple.GetValue(&plan_->InnerTableSchema(), i));
        }
        result_.push_back(Tuple(all_values, &GetOutputSchema()));
      }

      if(index_result.size() == 0 && plan_->GetJoinType() == JoinType::LEFT) {
        for(size_t i = 0; i<plan_->InnerTableSchema().GetColumnCount(); ++i) {
          values.push_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
        }
        result_.push_back(Tuple(values, &GetOutputSchema()));
      }
      
      statue = child_executor_->Next(&child_tuple, &rid);
    }
    
}

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  if(index_ == result_.size())
    return false;

  *tuple = result_[index_];
  ++index_;

  return true; 
}

}  // namespace bustub
