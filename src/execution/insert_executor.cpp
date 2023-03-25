//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  child_executor_->Init();
  count_ = 0;
  flag_ = true;
  // LOG_INFO("table insert");
  try {
    /* code */
    // 加锁  插入表时，对表加IX锁，然后插入
    // 不同隔离级别的差别？   怎么回滚呢？？
    // lock
    bool lockresult = exec_ctx_->GetLockManager()->LockTable(
        exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE, plan_->TableOid());
    if (!lockresult) {
      // LOG_INFO("lock table IX faile");
      exec_ctx_->GetTransaction()->SetState(TransactionState::ABORTED);
      throw std::exception();
    }
  } catch (const std::exception &e) {
    throw std::exception();
  }
  // LOG_INFO("lock table IX");
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
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
    // 1. 得到元组插入表
    TableInfo *tableinfo = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
    try {
      bool lockresult = exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(),
                                                             LockManager::LockMode::EXCLUSIVE, plan_->TableOid(), *rid);
      if (!lockresult) {
        // 对已经插入的进行rollback
        // LOG_INFO("lock row X faile");
        for (auto &rid : insert_recode_) {
          exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())->table_->ApplyDelete(rid, exec_ctx_->GetTransaction());
        }
        exec_ctx_->GetTransaction()->SetState(TransactionState::ABORTED);
        throw std::exception();
      }
    } catch (const std::exception &e) {
      // LOG_INFO("lock row X faile");
      for (auto &rid : insert_recode_) {
        exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())->table_->ApplyDelete(rid, exec_ctx_->GetTransaction());
      }
      throw std::exception();
    }
    // LOG_INFO("lock row X");
    bool result = tableinfo->table_->InsertTuple(child_tuple, rid, exec_ctx_->GetTransaction());
    assert(result != false);
    insert_recode_.emplace_back(*rid);
    // 2. 若有index，插入index
    auto indexinfos = exec_ctx_->GetCatalog()->GetTableIndexes(tableinfo->name_);
    for (auto &indexinfo : indexinfos) {
      // 获取对应列的值存入
      std::vector<Value> key_values{};
      key_values.reserve(indexinfo->key_schema_.GetColumnCount());
      for (auto &column_idx : indexinfo->index_->GetKeyAttrs()) {
        key_values.emplace_back(child_tuple.GetValue(&child_executor_->GetOutputSchema(), column_idx));
      }
      Tuple index_tuple = Tuple(key_values, &indexinfo->key_schema_);
      indexinfo->index_->InsertEntry(index_tuple, *rid, exec_ctx_->GetTransaction());
    }
    status = child_executor_->Next(&child_tuple, rid);
  }

  // 3. 插入完成后，返回插入完成的行数
  values[0] = Value{TypeId::INTEGER, count_};
  *tuple = Tuple(values, &GetOutputSchema());

  return true;
}

}  // namespace bustub
