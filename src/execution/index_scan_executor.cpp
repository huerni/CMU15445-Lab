//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      iterator_(dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(
                    exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid())->index_.get())
                    ->GetBeginIterator()) {}

void IndexScanExecutor::Init() {
  IndexInfo *indexinfo = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  tree_ = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(indexinfo->index_.get());
  iterator_ = tree_->GetBeginIterator();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iterator_ == tree_->GetEndIterator()) {
    return false;
  }

  // 找到rid后，从表中返回数据
  IndexInfo *indexinfo = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  TableInfo *tableinfo = exec_ctx_->GetCatalog()->GetTable(indexinfo->table_name_);
  *rid = (*iterator_).second;
  tableinfo->table_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction());
  ++iterator_;
  return true;
}

}  // namespace bustub
