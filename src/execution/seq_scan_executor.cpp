//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

#include "common/logger.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      iterator_(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_->Begin(exec_ctx_->GetTransaction())) {}

void SeqScanExecutor::Init() {
  // 获取table锁
  if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    bool result = exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(),
                                                         LockManager::LockMode::INTENTION_SHARED, plan_->GetTableOid());
    if (!result) {
      exec_ctx_->GetTransaction()->SetState(TransactionState::ABORTED);
      throw std::exception();
    }
    LOG_INFO("lock table IS");
  }
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto lock_manager = exec_ctx_->GetLockManager();
  auto transaction = exec_ctx_->GetTransaction();

  if (iterator_ == exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_->End()) {
    if (transaction->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      lock_manager->UnlockTable(transaction, plan_->GetTableOid());
      LOG_INFO("unlock table IS");
    }
    return false;
  }

  if (transaction->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    bool result =
        lock_manager->LockRow(transaction, LockManager::LockMode::SHARED, plan_->GetTableOid(), iterator_->GetRid());
    if (!result) {
      exec_ctx_->GetTransaction()->SetState(TransactionState::ABORTED);
      throw std::exception();
    }
    LOG_INFO("lock row S");
  }

  *tuple = Tuple(*iterator_);
  *rid = tuple->GetRid();
  ++iterator_;

  if (transaction->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    lock_manager->UnlockRow(transaction, plan_->GetTableOid(), iterator_->GetRid());
    LOG_INFO("unlock row S");
  }

  return true;
}

}  // namespace bustub
