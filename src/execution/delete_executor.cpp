//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  count_ = 0;
  flag_ = true;
  // LOG_INFO("table delete");
  // 对表获取IX锁，元组获取X锁
  try {
    /* code */
    bool result = exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(),
                                                         LockManager::LockMode::INTENTION_EXCLUSIVE, plan_->TableOid());
    if (!result) {
      exec_ctx_->GetTransaction()->SetState(TransactionState::ABORTED);
      throw std::exception();
    }
  } catch (const std::exception &e) {
    throw std::exception();
  }
  // LOG_INFO("lock table IX");
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  Tuple child_tuple{};
  auto status = child_executor_->Next(&child_tuple, rid);

  // 两次机会, !status不要立即返回结果
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());
  values.emplace_back(Value{TypeId::INTEGER, 0});
  *tuple = Tuple(values, &GetOutputSchema());

  if (flag_ && !status) {
    flag_ = false;
    return true;
  }

  flag_ = false;
  if (!status) {
    return false;
  }

  while (status) {
    ++count_;
    // 1. 得到元组从表中标记删除
    TableInfo *tableinfo = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
    try {
      bool result = exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE,
                                                         plan_->TableOid(), *rid);
      if (!result) {
        // 事务回滚
        for (auto &rid : remove_tuples_) {
          exec_ctx_->GetCatalog()
              ->GetTable(plan_->TableOid())
              ->table_->RollbackDelete(rid, exec_ctx_->GetTransaction());
        }
        exec_ctx_->GetTransaction()->SetState(TransactionState::ABORTED);
        throw std::exception();
      }
    } catch (const std::exception &e) {
      for (auto &rid : remove_tuples_) {
        exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())->table_->RollbackDelete(rid, exec_ctx_->GetTransaction());
      }
      throw std::exception();
    }
    // LOG_INFO("lock row X");
    tableinfo->table_->MarkDelete(*rid, exec_ctx_->GetTransaction());
    remove_tuples_.emplace_back(*rid);
    // 2. 若有index，从index删除
    auto indexinfos = exec_ctx_->GetCatalog()->GetTableIndexes(tableinfo->name_);
    for (auto &indexinfo : indexinfos) {
      std::vector<Value> key_values{};
      key_values.reserve(indexinfo->key_schema_.GetColumnCount());
      for (auto &column_idx : indexinfo->index_->GetKeyAttrs()) {
        key_values.push_back(child_tuple.GetValue(&child_executor_->GetOutputSchema(), column_idx));
      }
      Tuple index_tuple = Tuple(key_values, &indexinfo->key_schema_);
      indexinfo->index_->DeleteEntry(index_tuple, *rid, exec_ctx_->GetTransaction());
    }
    status = child_executor_->Next(&child_tuple, rid);
  }

  // 3. 插入完成后，返回插入完成的行数
  values[0] = Value{TypeId::INTEGER, count_};
  *tuple = Tuple(values, &GetOutputSchema());

  return true;
}
}  // namespace bustub
