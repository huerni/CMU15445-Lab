//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  // LOG_INFO("LockTable: %d %d %d %d", txn->GetTransactionId(), (int)txn->GetIsolationLevel(), (int)lock_mode, oid);
  /***
   * 1. 检查txn的状态
   *    - 若在COMMITTED，ABORTED阶段，直接返回false
   *    - 根据隔离级别和txn状态和添加的锁类型判断是否抛出异常
   *      - 若隔离级别为可重复读，且txn在SHRINKING阶段，抛出LOCK_ON_SHRINKING异常
   *      - 若隔离级别为提交后读，只有S,IS锁才能在SHRINKING阶段请求，否则抛异常
   *      - 若隔离级别为未提交读，不能请求S，IS，SIX锁，只能有GROWING阶段申请X,IX锁，否则抛异常。
   */
  if (txn->GetState() == TransactionState::COMMITTED || txn->GetState() == TransactionState::ABORTED) {
    throw std::logic_error("COMMITTED or ABORTED");
    // return false;
  }
  // 可重复读 两阶段
  if (txn->GetState() == TransactionState::SHRINKING && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }

  // 读已提交  收缩阶段只能申请IS, S锁
  if (txn->GetState() == TransactionState::SHRINKING && txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED &&
      lock_mode != LockMode::SHARED && lock_mode != LockMode::INTENTION_SHARED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }

  // 读未提交
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
        lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
    if (txn->GetState() == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }

  /***
   * 2. 获取锁请求队列，由tableoid获取，如果map中找不到，创建一个。
   */

  table_lock_map_latch_.lock();
  auto it = table_lock_map_.find(oid);
  if (it == table_lock_map_.end()) {
    table_lock_map_.insert({oid, std::make_shared<LockRequestQueue>()});
  }
  std::shared_ptr<LockRequestQueue> lock_request_queue = table_lock_map_[oid];

  /***
   * 3. 判断事务是否在队列中，在的话进行锁升级操作，不在的话加入队列
   *    - 锁升级时
   *      - 如果lockmode与持有的锁一致，直接返回true
   *      - 如果不一致，进行可行规则升级，否则ABORTED，抛出INCOMPATIBLE_UPGRADE
   *      - 同一时间只能一个事务进行升级，否则ABORTED，抛出UPGRADE_CONFLICT异常
   *      - 没问题，就把需更新的事务id改成请求的事务id，进行锁升级
   *    - 加入锁时
   *      - 创建LockRequest，加入队列
   */

  // 先锁住再释放
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
  table_lock_map_latch_.unlock();
  auto request_queue = lock_request_queue->request_queue_;
  auto iter = request_queue.begin();
  std::shared_ptr<LockRequest> lock_requset = nullptr;
  for (; iter != request_queue.end(); ++iter) {
    if ((*iter)->txn_id_ == txn->GetTransactionId()) {
      lock_requset = (*iter);
      break;
    }
  }

  // 升级锁
  if (lock_requset != nullptr) {
    // 没有被授予
    if (!lock_requset->granted_) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
    }
    // 相等，无需升级
    if (lock_requset->lock_mode_ == lock_mode) {
      return true;
    }

    if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    }
    // IS -> [S, X, IX, SIX]

    // S -> [X, SIX]
    if (lock_requset->lock_mode_ == LockMode::SHARED && lock_mode != LockMode::EXCLUSIVE &&
        lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
    }
    // IX -> [X, SIX]
    if (lock_requset->lock_mode_ == LockMode::INTENTION_EXCLUSIVE && lock_mode != LockMode::EXCLUSIVE &&
        lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
    }
    // SIX -> [X]
    if (lock_requset->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE && lock_mode != LockMode::EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
    }
    if (lock_requset->lock_mode_ == LockMode::EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
    }

    // 释放锁，准备重新加入锁进行锁升级
    auto old_lock_mode = lock_requset->lock_mode_;
    switch (old_lock_mode) {
      case LockMode::EXCLUSIVE:
        txn->GetExclusiveTableLockSet()->erase(oid);
        break;
      case LockMode::SHARED:
        txn->GetSharedTableLockSet()->erase(oid);
        break;
      case LockMode::INTENTION_EXCLUSIVE:
        txn->GetIntentionExclusiveTableLockSet()->erase(oid);
        break;
      case LockMode::INTENTION_SHARED:
        txn->GetIntentionSharedTableLockSet()->erase(oid);
        break;
      case LockMode::SHARED_INTENTION_EXCLUSIVE:
        txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
        break;
    }
    lock_request_queue->request_queue_.remove(lock_requset);
    lock_request_queue->upgrading_ = txn->GetTransactionId();
  }

  std::shared_ptr<LockRequest> new_lock_request =
      std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
  lock_request_queue->request_queue_.emplace_back(new_lock_request);

  /***
   * 4. 开始尝试获取锁
   *    - 新创建LockRequest加入队列
   *    - 使用条件变量分析是否可以加入锁
   *    - 若为升级操作，则优先级最高，直接更新锁
   *    - 若为加锁操作，则遍历队列，查看优先级和兼容度
   */

  // 当条件不满足时，自动释放锁，并挂起
  while (!GrantTableLock(lock_request_queue, new_lock_request)) {
    lock_request_queue->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      return false;
    }
  }

  /***
   * 5. 向txn中添加记录，锁升级时需删除旧锁记录
   */
  switch (lock_mode) {
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->insert(oid);
      break;
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->insert(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->insert(oid);
      break;
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->insert(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);
      break;
  }

  return true;
}

auto LockManager::GrantTableLock(std::shared_ptr<LockRequestQueue> &lock_request_queue,
                                 std::shared_ptr<LockRequest> &lock_request) -> bool {
  // FIX FINISH: X锁应该等待，与S锁不兼容 更新时首先判断与已授予的是否兼容
  auto request_queue = lock_request_queue->request_queue_;
  for (auto &iter : request_queue) {
    if (iter->granted_ && !CheckCompatibility(iter->lock_mode_, lock_request->lock_mode_)) {
      return false;
    }
  }

  if (lock_request_queue->upgrading_ == lock_request->txn_id_) {
    lock_request_queue->upgrading_ = INVALID_TXN_ID;
    lock_request->granted_ = true;
    return true;
  }

  if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
    return false;
  }

  // 再判断优先级
  for (auto &iter : request_queue) {
    if (iter->txn_id_ == lock_request->txn_id_) {
      lock_request->granted_ = true;
      return true;
    }

    if (!iter->granted_ && !CheckCompatibility(iter->lock_mode_, lock_request->lock_mode_)) {
      return false;
    }
  }

  return false;
}

auto LockManager::CheckCompatibility(LockMode hold_mode, LockMode want_mode) -> bool {
  // IS > X
  if (hold_mode == LockMode::INTENTION_SHARED && want_mode == LockMode::EXCLUSIVE) {
    return false;
  }
  // IX > S,SIX,X
  if (hold_mode == LockMode::INTENTION_EXCLUSIVE &&
      (want_mode == LockMode::SHARED || want_mode == LockMode::SHARED_INTENTION_EXCLUSIVE ||
       want_mode == LockMode::EXCLUSIVE)) {
    return false;
  }
  // S > IX, SIX, X
  if (hold_mode == LockMode::SHARED &&
      (want_mode == LockMode::INTENTION_EXCLUSIVE || want_mode == LockMode::SHARED_INTENTION_EXCLUSIVE ||
       want_mode == LockMode::EXCLUSIVE)) {
    return false;
  }
  // SIX > IX, S, SIX, X
  if (hold_mode == LockMode::SHARED_INTENTION_EXCLUSIVE && want_mode != LockMode::INTENTION_SHARED) {
    return false;
  }
  // X > IS, IX, S, SIX, X
  if (hold_mode == LockMode::EXCLUSIVE) {
    return false;
  }

  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  // LOG_INFO("UnlockTable: %d %d", txn->GetTransactionId(), oid);
  /***
   * 判断是否有行级锁未释放
   */
  if (!((*txn->GetExclusiveRowLockSet())[oid].empty() && (*txn->GetSharedRowLockSet())[oid].empty())) {
    LOG_INFO("row lock is unlock");
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }

  table_lock_map_latch_.lock();
  auto it = table_lock_map_.find(oid);
  if (it == table_lock_map_.end()) {
    table_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  auto lock_request_queue = table_lock_map_[oid];

  std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
  table_lock_map_latch_.unlock();
  std::list<std::shared_ptr<LockRequest>> request_queue = lock_request_queue->request_queue_;
  std::shared_ptr<LockRequest> lock_request = nullptr;
  auto iter = request_queue.begin();
  for (; iter != request_queue.end(); ++iter) {
    if ((*iter)->txn_id_ == txn->GetTransactionId()) {
      lock_request = (*iter);
      break;
    }
  }

  if (lock_request == nullptr) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  if (txn->GetState() == TransactionState::GROWING) {
    if ((txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && lock_request->lock_mode_ == LockMode::SHARED) ||
        lock_request->lock_mode_ == LockMode::EXCLUSIVE) {
      txn->SetState(TransactionState::SHRINKING);
    }
  }

  auto lock_mode = lock_request->lock_mode_;
  lock_request_queue->request_queue_.remove(lock_request);
  switch (lock_mode) {
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->erase(oid);
      break;
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->erase(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->erase(oid);
      break;
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->erase(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
      break;
  }

  lock_request_queue->cv_.notify_all();

  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  /***
   * 检查表级索是否加锁，其余与locktable操作差不多
   */
  // LOG_INFO("LockRow: %d %d %d %s", txn->GetTransactionId(), (int)lock_mode, oid, rid.ToString().c_str());
  if (txn->GetState() == TransactionState::ABORTED || txn->GetState() == TransactionState::COMMITTED) {
    throw std::logic_error("COMMITTED or ABORTED");
    // return false;
  }

  // 行级锁只能加S/X锁
  assert(lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED);
  if (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::SHARED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }

  if (lock_mode == LockMode::EXCLUSIVE) {
    if (!txn->IsTableExclusiveLocked(oid) && !txn->IsTableIntentionExclusiveLocked(oid) &&
        !txn->IsTableSharedIntentionExclusiveLocked(oid)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  }

  if (txn->GetState() == TransactionState::SHRINKING) {
    if (!(txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && lock_mode == LockMode::SHARED)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }

  row_lock_map_latch_.lock();
  auto it = row_lock_map_.find(rid);
  if (it == row_lock_map_.end()) {
    row_lock_map_.insert({rid, std::make_shared<LockRequestQueue>()});
  }
  auto lock_request_queue = row_lock_map_[rid];

  std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
  row_lock_map_latch_.unlock();
  auto request_queue = lock_request_queue->request_queue_;
  std::shared_ptr<LockRequest> lock_request = nullptr;
  auto iter = request_queue.begin();
  for (; iter != request_queue.end(); ++iter) {
    if ((*iter)->txn_id_ == txn->GetTransactionId() && (*iter)->oid_ == oid) {
      lock_request = (*iter);
      break;
    }
  }

  if (lock_request != nullptr) {
    if (!lock_request->granted_) {
      return false;
    }
    if (lock_request->lock_mode_ == lock_mode) {
      return true;
    }
    if (lock_mode == LockMode::SHARED) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
    }
    lock_request_queue->upgrading_ = lock_request->txn_id_;
    lock_request_queue->request_queue_.remove(lock_request);
  }

  auto new_lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
  lock_request_queue->request_queue_.emplace_back(new_lock_request);

  while (!GrantRowLock(lock_request_queue, new_lock_request)) {
    lock_request_queue->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      return false;
    }
  }

  // txn状态持久化
  if (lock_mode == LockMode::SHARED) {
    auto ms = txn->GetSharedRowLockSet();
    auto it = ms->find(oid);
    if (it == ms->end()) {
      ms->insert({oid, std::unordered_set<RID>()});
      (*ms)[oid].insert(rid);
    } else {
      it->second.insert(rid);
    }
  } else if (lock_mode == LockMode::EXCLUSIVE) {
    auto ms = txn->GetExclusiveRowLockSet();
    auto it = ms->find(oid);
    if (it == ms->end()) {
      ms->insert({oid, std::unordered_set<RID>()});
      (*ms)[oid].insert(rid);
    } else {
      it->second.insert(rid);
    }
  }

  return true;
}

auto LockManager::GrantRowLock(std::shared_ptr<LockRequestQueue> &lock_request_queue,
                               std::shared_ptr<LockRequest> &lock_request) -> bool {
  // 首先判断兼容性
  auto request_queue = lock_request_queue->request_queue_;
  for (auto &iter : request_queue) {
    if (iter->granted_ && !CheckCompatibility(iter->lock_mode_, lock_request->lock_mode_)) {
      return false;
    }
  }
  // 是否升级
  if (lock_request_queue->upgrading_ == lock_request->txn_id_) {
    lock_request_queue->upgrading_ = INVALID_TXN_ID;
    lock_request->granted_ = true;
    return true;
  }

  if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
    return false;
  }

  for (auto &iter : request_queue) {
    if (iter->txn_id_ == lock_request->txn_id_ && iter->oid_ == lock_request->oid_) {
      lock_request->granted_ = true;
      return true;
    }
    if (!iter->granted_ && !CheckCompatibility(iter->lock_mode_, lock_request->lock_mode_)) {
      return false;
    }
  }

  return false;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  row_lock_map_latch_.lock();

  // LOG_INFO("UnlockRow: %d %d %s", txn->GetTransactionId(), oid, rid.ToString().c_str());
  auto it = row_lock_map_.find(rid);
  if (it == row_lock_map_.end()) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  auto lock_request_queue = row_lock_map_[rid];

  std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
  row_lock_map_latch_.unlock();
  auto iter = lock_request_queue->request_queue_.begin();
  std::shared_ptr<LockRequest> lock_request = nullptr;
  for (; iter != lock_request_queue->request_queue_.end(); ++iter) {
    if ((*iter)->txn_id_ == txn->GetTransactionId() && (*iter)->oid_ == oid) {
      lock_request = (*iter);
      break;
    }
  }

  if (lock_request == nullptr) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  if (txn->GetState() == TransactionState::GROWING) {
    if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ ||
        (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED &&
         lock_request->lock_mode_ == LockMode::EXCLUSIVE)) {
      txn->SetState(TransactionState::SHRINKING);
    }
  }

  auto lock_mode = lock_request->lock_mode_;
  lock_request_queue->request_queue_.remove(lock_request);

  // txn状态持久化
  if (lock_mode == LockMode::SHARED) {
    auto it = txn->GetSharedRowLockSet()->find(oid);
    it->second.erase(rid);
  } else if (lock_mode == LockMode::EXCLUSIVE) {
    auto it = txn->GetExclusiveRowLockSet()->find(oid);
    it->second.erase(rid);
  }

  lock_request_queue->cv_.notify_all();

  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  auto it = waits_for_.find(t1);
  if (it == waits_for_.end()) {
    waits_for_.insert({t1, std::vector<txn_id_t>()});
  }
  it = waits_for_.find(t1);
  auto vec = it->second;
  auto iter = std::lower_bound(vec.begin(), vec.end(), t2);
  if (iter == vec.end() || (*iter) != t2) {
    it->second.push_back(t2);
  }
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  auto it = waits_for_.find(t1);
  if (it != waits_for_.end()) {
    it->second.erase(std::remove(it->second.begin(), it->second.end(), t2), it->second.end());
    if (it->second.empty()) {
      waits_for_.erase(t1);
    }
  }
}

auto LockManager::SearchCycle(std::unordered_set<txn_id_t> &isvisited, txn_id_t cur, std::vector<txn_id_t> &ans)
    -> bool {
  // 找到环里面最年轻(id最大)的txn_id
  auto vec = waits_for_[cur];
  bool is_cycle = false;
  for (auto &ne : vec) {
    if (isvisited.count(ne) != 0U) {
      is_cycle = true;
      ans = std::vector<txn_id_t>(isvisited.begin(), isvisited.end());
      break;
    }
    isvisited.insert(ne);
    is_cycle |= SearchCycle(isvisited, ne, ans);
    isvisited.erase(ne);
    // 找到环
    if (is_cycle) {
      break;
    }
  }

  return is_cycle;
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  if (waits_for_.empty()) {
    return false;
  }

  std::vector<txn_id_t> sort_keys;
  for (auto &it : waits_for_) {
    sort_keys.emplace_back(it.first);
  }
  std::sort(sort_keys.begin(), sort_keys.end());
  // 从tid较小的id开始搜索
  bool result = false;
  for (auto &key : sort_keys) {
    std::vector<txn_id_t> ans;
    std::unordered_set<txn_id_t> isvisited;
    isvisited.insert(key);
    result = SearchCycle(isvisited, key, ans);
    if (result) {
      *txn_id = *std::max_element(ans.begin(), ans.end());
      break;
    }
  }

  return result;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);

  for (auto &it : waits_for_) {
    for (auto &val : it.second) {
      edges.emplace_back(it.first, val);
    }
  }

  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
      /***
       * 1. 构建图
       * 2. 循环检测环，并abort环中最小txn
       * 3. 销毁图
       */
      waits_for_latch_.lock();
      LOG_INFO("RunCycleDetection");
      // table
      table_lock_map_latch_.lock();
      for (auto &it : table_lock_map_) {
        for (auto &iter1 : it.second->request_queue_) {
          if (!iter1->granted_) {
            for (auto &iter2 : it.second->request_queue_) {
              if (iter2->granted_) {
                if (!CheckCompatibility(iter1->lock_mode_, iter2->lock_mode_)) {
                  AddEdge(iter1->txn_id_, iter2->txn_id_);
                }
              }
            }
          }
        }
      }
      // table_lock_map_latch_.unlock();
      // row
      row_lock_map_latch_.lock();
      for (auto &it : row_lock_map_) {
        for (auto &iter1 : it.second->request_queue_) {
          if (!iter1->granted_) {
            for (auto &iter2 : it.second->request_queue_) {
              if (iter2->granted_) {
                if (!CheckCompatibility(iter1->lock_mode_, iter2->lock_mode_)) {
                  AddEdge(iter1->txn_id_, iter2->txn_id_);
                }
              }
            }
          }
        }
      }

      for (auto &it : waits_for_) {
        std::sort(it.second.begin(), it.second.end());
      }
      // 检测
      txn_id_t txn_id = -1;
      while (HasCycle(&txn_id)) {
        assert(txn_id != -1);
        LOG_INFO("REMOVE: %d", txn_id);
        auto txn = TransactionManager::GetTransaction(txn_id);
        txn->SetState(TransactionState::ABORTED);

        // 删除指定txn_id
        // table_lock_map_latch_.lock();
        for (auto &it : table_lock_map_) {
          std::unique_lock<std::mutex> lock(it.second->latch_);
          /**
           * auto iter = it.second->request_queue_.begin();
          while (iter != it.second->request_queue_.end()) {
            if ((*iter)->txn_id_ == txn_id) {
              // 持久化
              auto lock_mode = (*iter)->lock_mode_;
              auto oid = (*iter)->oid_;
              switch(lock_mode) {
                case LockMode::EXCLUSIVE:
                  txn->GetExclusiveTableLockSet()->erase(oid);
                  break;
                case LockMode::SHARED:
                  txn->GetSharedTableLockSet()->erase(oid);
                  break;
                case LockMode::INTENTION_EXCLUSIVE:
                  txn->GetIntentionExclusiveTableLockSet()->erase(oid);
                  break;
                case LockMode::INTENTION_SHARED:
                  txn->GetIntentionSharedTableLockSet()->erase(oid);
                  break;
                default :
                  txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
                  break;
              }
              iter = it.second->request_queue_.erase(iter);
            } else {
              iter++;
            }
          }
          */
          it.second->cv_.notify_all();
        }
        // table_lock_map_latch_.unlock();

        // row_lock_map_latch_.lock();
        for (auto &it : row_lock_map_) {
          std::unique_lock<std::mutex> lock(it.second->latch_);
          /***
           * auto iter = it.second->request_queue_.begin();
          while (iter != it.second->request_queue_.end()) {
            if ((*iter)->txn_id_ == txn_id) {
              // 持久化
              auto lock_mode = (*iter)->lock_mode_;
              auto oid = (*iter)->oid_;
              auto rid = (*iter)->rid_;
              switch(lock_mode) {
                case LockMode::SHARED:
                  txn->GetSharedRowLockSet()->find(oid)->second.erase(rid);
                  break;
                case LockMode::EXCLUSIVE:
                  txn->GetExclusiveRowLockSet()->find(oid)->second.erase(rid);
                  break;
                default:
                  break;
              }
              iter = it.second->request_queue_.erase(iter);
            } else {
              iter++;
            }
          }
          */
          it.second->cv_.notify_all();
        }
        // row_lock_map_latch_.unlock();

        // 删除边
        waits_for_.erase(txn_id);
        std::vector<txn_id_t> keys;
        for (auto &it : waits_for_) {
          keys.push_back(it.first);
        }
        for (auto &key : keys) {
          RemoveEdge(key, txn_id);
        }
      }

      // 删除
      waits_for_.clear();
      row_lock_map_latch_.unlock();
      table_lock_map_latch_.unlock();
      waits_for_latch_.unlock();
    }
  }
}

}  // namespace bustub
