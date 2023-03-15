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
  /***
   * 1. 检查txn的状态
   *    - 若在COMMITTED，ABORTED阶段，抛出逻辑异常，直接返回false
   *    - 根据隔离级别和txn状态和添加的锁类型判断是否抛出异常
   *      - 若隔离级别为可重复读，且txn在SHRINKING阶段，抛出LOCK_ON_SHRINKING异常
   *      - 若隔离级别为提交后读，只有S,IS锁才能在SHRINKING阶段请求，否则抛异常
   *      - 若隔离级别为未提交读，不能请求S，IS，SIX锁，只能有GROWING阶段申请X,IX锁，否则抛异常。
  */
  if(txn->GetState() == TransactionState::COMMITTED || txn->GetState() == TransactionState::ABORTED) {
    throw std::logic_error("COMMITTED or ABORTED");
  }
  if(txn->GetState() == TransactionState::SHRINKING && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }
  if(txn->GetState() == TransactionState::SHRINKING && txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && lock_mode != LockMode::SHARED && lock_mode != LockMode::INTENTION_SHARED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }
  if(txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    if(lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
    if(txn->GetState() == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }

  /***
   * 2. 获取锁请求队列，由tableoid获取，如果map中找不到，创建一个。
  */
  table_lock_map_latch_.lock();
  auto it = table_lock_map_.find(oid);
  if(it == table_lock_map_.end()) {
    table_lock_map_.insert({oid, std::make_shared<LockRequestQueue>()});
  }
  std::shared_ptr<LockRequestQueue> lock_request_queue = table_lock_map_[oid];
  table_lock_map_latch_.unlock();
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

  std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
  auto request_queue = lock_request_queue->request_queue_;
  std::list<std::shared_ptr<LockRequest>>::iterator ite = request_queue.begin();
  std::shared_ptr<LockRequest> lock_requset = nullptr;
  for(; ite != request_queue.end(); ++ite) {
    if((*ite)->txn_id_ == txn->GetTransactionId()) {
      lock_requset = (*ite);
      break;
    }
  }

  // 升级锁
  if(lock_requset != nullptr) {
    if(lock_requset->lock_mode_ == lock_mode) {
      lock_request_queue->latch_.unlock();
      return true;
    }
    else {
      if(lock_requset->granted_ == false) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      // IS -> [S, X, IX, SIX]
      if(lock_requset->lock_mode_ == LockMode::INTENTION_SHARED) {
          txn->SetState(TransactionState::ABORTED);
          throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      // S -> [X, SIX]
      if(lock_requset->lock_mode_ == LockMode::SHARED && lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      // IX -> [X, SIX]
      if(lock_requset->lock_mode_ == LockMode::INTENTION_EXCLUSIVE && lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      // SIX -> [X]
      if(lock_requset->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE && lock_mode != LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      if(lock_request_queue->upgrading_ != INVALID_TXN_ID) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }
      // 释放锁，准备重新加入锁进行锁升级
      auto old_lock_mode = lock_requset->lock_mode_;
      if(old_lock_mode == LockMode::EXCLUSIVE) {
        txn->GetExclusiveTableLockSet()->erase(oid);
      }
      else if(old_lock_mode == LockMode::SHARED) {
        txn->GetSharedTableLockSet()->erase(oid);
      }
      else if(old_lock_mode == LockMode::INTENTION_EXCLUSIVE) {
        txn->GetIntentionExclusiveTableLockSet()->erase(oid);
      }
      else if(old_lock_mode == LockMode::INTENTION_SHARED) {
        txn->GetIntentionSharedTableLockSet()->erase(oid);
      }
      else {
        txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
      }
      lock_request_queue->request_queue_.remove(lock_requset);
      lock_request_queue->upgrading_ = txn->GetTransactionId();
    }
  }

  std::shared_ptr<LockRequest> new_lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
  lock_request_queue->request_queue_.emplace_back(new_lock_request);

  /***
    * 4. 开始尝试获取锁
    *    - 新创建LockRequest加入队列
    *    - 使用条件变量分析是否可以加入锁
    *    - 若为升级操作，则优先级最高，直接更新锁
    *    - 若为加锁操作，则遍历队列，查看优先级和兼容度
  */
  
  // 当条件不满足时，自动释放锁，并挂起
  while(!GrantTableLock(lock_request_queue, new_lock_request)) {
    lock_request_queue->cv_.wait(lock);
    if(txn->GetState() == TransactionState::ABORTED) {
      return false;
    }
  }

  /***
   * 5. 向txn中添加记录，锁升级时需删除旧锁记录
  */
  if(lock_mode == LockMode::EXCLUSIVE) {
    txn->GetExclusiveTableLockSet()->insert(oid);
  }
  else if(lock_mode == LockMode::SHARED) {
    txn->GetSharedTableLockSet()->insert(oid);
  }
  else if(lock_mode == LockMode::INTENTION_EXCLUSIVE) {
    txn->GetIntentionExclusiveTableLockSet()->insert(oid);
  }
  else if(lock_mode == LockMode::INTENTION_SHARED) {
    txn->GetIntentionSharedTableLockSet()->insert(oid);
  }
  else {
    txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);
  }

  return true; 
}

auto LockManager::GrantTableLock(std::shared_ptr<LockRequestQueue> lock_request_queue, std::shared_ptr<LockRequest> lock_request) -> bool {
  if(lock_request_queue->upgrading_ == lock_request->txn_id_) {
    lock_request_queue->upgrading_ = INVALID_TXN_ID;
    lock_request->granted_ = true;
    return true;
  }

  if(lock_request_queue->upgrading_ != INVALID_TXN_ID) {
    return false;
  }

  auto request_queue = lock_request_queue->request_queue_;
  std::list<std::shared_ptr<LockRequest>>::iterator iter = request_queue.begin();
  for(; iter != request_queue.end(); ++iter) {
    if((*iter)->txn_id_ == lock_request->txn_id_) {
      lock_request->granted_ = true;
      return true;
    }
    if((*iter)->granted_ == false) {
      continue;
    }
    if(!CheckCompatibility((*iter)->lock_mode_, lock_request->lock_mode_)) {
      return false;
    }
  }

  return false;
}

auto LockManager::CheckCompatibility(LockMode hold_mode, LockMode want_mode) -> bool {
  // IS > X
    if(hold_mode == LockMode::INTENTION_SHARED && want_mode == LockMode::EXCLUSIVE) {
      return false;
    }
    // IX > S,SIX,X
    if(hold_mode == LockMode::INTENTION_EXCLUSIVE 
        && (want_mode == LockMode::SHARED 
            || want_mode == LockMode::SHARED_INTENTION_EXCLUSIVE 
            || want_mode == LockMode::EXCLUSIVE)) {
      return false;
    }
    // S > IX, SIX, X
    if(hold_mode == LockMode::SHARED 
        && (want_mode == LockMode::INTENTION_EXCLUSIVE
            || want_mode == LockMode::SHARED_INTENTION_EXCLUSIVE
            || want_mode == LockMode::EXCLUSIVE)) {
      return false;
    }
    // SIX > IX, S, SIX, X
    if(hold_mode == LockMode::SHARED_INTENTION_EXCLUSIVE
        && (want_mode == LockMode::INTENTION_EXCLUSIVE
            || want_mode == LockMode::SHARED
            || want_mode == LockMode::SHARED_INTENTION_EXCLUSIVE
            || want_mode == LockMode::EXCLUSIVE)) {
      return false;
    }
    // X > IS, IX, S, SIX, X
    if(hold_mode == LockMode::EXCLUSIVE) {
      return false;
    }

    return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {

  /***
   * 判断是否有行级锁未释放
  */
  if(!((*txn->GetExclusiveRowLockSet())[oid].empty() && (*txn->GetSharedRowLockSet())[oid].empty())) {
    int i = 1; 
    assert(i != 1);
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }
  
  table_lock_map_latch_.lock();
  auto it = table_lock_map_.find(oid);
  if(it == table_lock_map_.end()) {
    txn->SetState(TransactionState::ABORTED);
    table_lock_map_latch_.unlock();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  auto lock_request_queue = table_lock_map_[oid];
  table_lock_map_latch_.unlock();

  std::unique_lock<std::mutex> lock(lock_request_queue->latch_);

  std::list<std::shared_ptr<LockRequest>> request_queue = lock_request_queue->request_queue_;
  std::shared_ptr<LockRequest> lock_request = nullptr;
  std::list<std::shared_ptr<LockRequest>>::iterator iter = request_queue.begin();
  for(; iter != request_queue.end(); ++iter) {
    if((*iter)->txn_id_ == txn->GetTransactionId()) {
      lock_request = (*iter);
      break;
    }
  }

  if(lock_request == nullptr) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  if(txn->GetState() == TransactionState::GROWING && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && (lock_request->lock_mode_ == LockMode::EXCLUSIVE || lock_request->lock_mode_ == LockMode::SHARED)) {
    txn->SetState(TransactionState::SHRINKING);
  }
  else if(txn->GetState() == TransactionState::GROWING && txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && lock_request->lock_mode_ == LockMode::EXCLUSIVE) {
    txn->SetState(TransactionState::SHRINKING);
  }
  else if(txn->GetState() == TransactionState::GROWING && txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED && lock_request->lock_mode_ == LockMode::EXCLUSIVE) {
    txn->SetState(TransactionState::SHRINKING);
  }

  auto lock_mode = lock_request->lock_mode_;
  lock_request_queue->request_queue_.remove(lock_request);

  if(lock_mode == LockMode::EXCLUSIVE) {
    txn->GetExclusiveTableLockSet()->erase(oid);
  }
  else if(lock_mode == LockMode::SHARED) {
    txn->GetSharedTableLockSet()->erase(oid);
  }
  else if(lock_mode == LockMode::INTENTION_EXCLUSIVE) {
    txn->GetIntentionExclusiveTableLockSet()->erase(oid);
  }
  else if(lock_mode == LockMode::INTENTION_SHARED) {
    txn->GetIntentionSharedTableLockSet()->erase(oid);
  }
  else {
    txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
  }

  lock_request_queue->cv_.notify_all();

  return true; 
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  /***
   * 检查表级索是否加锁，其余与locktable操作差不多
  */
  
  if(txn->GetState() == TransactionState::ABORTED || txn->GetState() == TransactionState::COMMITTED) {
    throw std::logic_error("COMMITTED or ABORTED");
  }
  
  // 行级锁只能加S/X锁
  assert(lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED);
  if(lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::SHARED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }
  
  if(lock_mode == LockMode::EXCLUSIVE) {
    if(!txn->IsTableExclusiveLocked(oid) && !txn->IsTableIntentionExclusiveLocked(oid) && !txn->IsTableSharedIntentionExclusiveLocked(oid)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  }
  
  if(txn->GetState() == TransactionState::SHRINKING) {
    if(!(txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && lock_mode == LockMode::SHARED)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  
  row_lock_map_latch_.lock();
  auto it = row_lock_map_.find(rid);
  if(it == row_lock_map_.end()) {
    row_lock_map_.insert({rid, std::make_shared<LockRequestQueue>()});
  }
  auto lock_request_queue = row_lock_map_[rid];
  row_lock_map_latch_.unlock();
  
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
  auto request_queue = lock_request_queue->request_queue_;
  std::shared_ptr<LockRequest> lock_request = nullptr;
  std::list<std::shared_ptr<LockRequest>>::iterator iter = request_queue.begin();
  for(; iter != request_queue.end(); ++iter) {
    if((*iter)->txn_id_ == txn->GetTransactionId() && (*iter)->oid_ == oid) {
      lock_request = (*iter);
      break;
    }
  }

  if(lock_request != nullptr) {
    if(lock_request->lock_mode_ == lock_mode) {
      return true;
    }
    if(lock_mode == LockMode::SHARED) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
    }
    lock_request_queue->upgrading_ = lock_request->txn_id_;
    lock_request_queue->request_queue_.remove(lock_request);
  }
  
  auto new_lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
  lock_request_queue->request_queue_.emplace_back(new_lock_request);
  
  while(!GrantRowLock(lock_request_queue, new_lock_request)) {
    lock_request_queue->cv_.wait(lock);
    if(txn->GetState() == TransactionState::ABORTED) {
      return false;
    }
  }
  
  // txn状态持久化
  if(lock_mode == LockMode::SHARED) {
    auto ms = txn->GetSharedRowLockSet();
    auto it = ms->find(oid);
    if(it == ms->end()) {
      ms->insert({oid, std::unordered_set<RID>()});
      (*ms)[oid].insert(rid);
    }
    else {
      it->second.insert(rid);
    }
  }
  else if(lock_mode == LockMode::EXCLUSIVE) {
    auto ms = txn->GetExclusiveRowLockSet();
    auto it = ms->find(oid);
    if(it == ms->end()) {
      ms->insert({oid, std::unordered_set<RID>()});
      (*ms)[oid].insert(rid);
    }
    else {
      it->second.insert(rid);
    }
  }
  
  return true;
}

auto LockManager::GrantRowLock(std::shared_ptr<LockRequestQueue> lock_request_queue, std::shared_ptr<LockRequest> lock_request) -> bool {
  // 是否升级
  if(lock_request_queue->upgrading_ == lock_request->txn_id_) {
    lock_request_queue->upgrading_ = INVALID_TXN_ID;
    lock_request->granted_ = true;
    return true;
  }

  if(lock_request_queue->upgrading_ != INVALID_TXN_ID) {
    return false;
  }

  std::list<std::shared_ptr<LockRequest>>::iterator iter = lock_request_queue->request_queue_.begin();
  for(; iter != lock_request_queue->request_queue_.end(); ++iter) {
     if((*iter)->txn_id_ == lock_request->txn_id_ && (*iter)->oid_ == lock_request->oid_) {
      lock_request->granted_ = true;
      return true;
    }
    if((*iter)->granted_ == false) {
      continue;
    }
    if((*iter)->lock_mode_ == LockMode::EXCLUSIVE) {
      return false;
    }
    if((*iter)->lock_mode_ == LockMode::SHARED && lock_request->lock_mode_ == LockMode::EXCLUSIVE) {
      return false;
    }
  }

  return false;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool { 
  
  row_lock_map_latch_.lock();
  auto it = row_lock_map_.find(rid);
  if(it == row_lock_map_.end()) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  auto lock_request_queue = row_lock_map_[rid];
  row_lock_map_latch_.unlock();
  
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
  std::list<std::shared_ptr<LockRequest>>::iterator iter = lock_request_queue->request_queue_.begin();
  std::shared_ptr<LockRequest> lock_request = nullptr;
  for(; iter != lock_request_queue->request_queue_.end(); ++iter) {
    if((*iter)->txn_id_ == txn->GetTransactionId() && (*iter)->oid_ == oid) {
      lock_request = (*iter);
      break;
    }
  }
  
  LOG_INFO("111111");
  // TODO: 为啥抛异常？？
  if(lock_request == nullptr) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  
  if(txn->GetState() == TransactionState::GROWING && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    txn->SetState(TransactionState::SHRINKING);
  }
  else if(txn->GetState() == TransactionState::GROWING && txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && lock_request->lock_mode_ == LockMode::EXCLUSIVE) {
    txn->SetState(TransactionState::SHRINKING);
  }

  auto lock_mode = lock_request->lock_mode_;
  lock_request_queue->request_queue_.remove(lock_request);

  // txn状态持久化
  if(lock_mode == LockMode::SHARED) {
    auto it = txn->GetSharedRowLockSet()->find(oid);
    it->second.erase(rid);
    if(it->second.empty()) {
      txn->GetSharedRowLockSet()->erase(oid);
    }
  }
  else if(lock_mode == LockMode::EXCLUSIVE) {
    auto it = txn->GetExclusiveRowLockSet()->find(oid);
    it->second.erase(rid);
    if(it->second.empty()) {
      txn->GetExclusiveRowLockSet()->erase(oid);
    }
  }

  lock_request_queue->cv_.notify_all();
  
  return true; 
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  auto it = waits_for_.find(t1);
  if(it == waits_for_.end()) {
    waits_for_.insert({t1, std::vector<txn_id_t>()});
  }
  it = waits_for_.find(t1);
  auto vec = it->second;
  auto iter = std::lower_bound(vec.begin(), vec.end(), t2);
  if(iter == vec.end() || (*iter) != t2) {
    it->second.push_back(t2);
  }
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  auto it = waits_for_.find(t1);
  if(it != waits_for_.end()) {
    it->second.erase(std::remove(it->second.begin(),  it->second.end(), t2), it->second.end());
    if(it->second.empty()) {
      waits_for_.erase(t1);
    }
  }
}

auto LockManager::SearchCycle(std::unordered_set<txn_id_t> &isvisited, txn_id_t cur, std::vector<txn_id_t>& ans) -> bool {
  //找到环里面最年轻(id最大)的txn_id

  auto vec = waits_for_[cur];
  bool isCycle = false;
  for(auto& ne : vec) {
    if(isvisited.count(ne)) {
      isCycle = true;
      ans = std::vector<txn_id_t>(isvisited.begin(), isvisited.end());
      break;
    }
    isvisited.insert(ne);
    isCycle |= SearchCycle(isvisited, ne, ans);
    isvisited.erase(ne);
    // 找到环
    if(isCycle == true)
      break;
  }

  return isCycle;
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool { 
  if(waits_for_.size() == 0) {
    return false;
  }

  std::unordered_set<txn_id_t> isvisited;
  txn_id_t min_id = -1;
  for(auto& [k, v] : waits_for_) {
    LOG_INFO("%d : ", k);
    for(auto& t : v) {
      LOG_INFO("%d, ", t);
    }
    if(min_id == -1) {
      min_id = k;
    }
    else {
      min_id = std::min(min_id, k);
    }
  }
  LOG_INFO("-------");
  std::vector<txn_id_t> ans;
  isvisited.insert(min_id);
  bool result = SearchCycle(isvisited, min_id, ans);
  for(auto& t_id : ans) {
    *txn_id = std::max(t_id, *txn_id);
  }
  return result;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);

  for(auto& [k, v] : waits_for_) {
    for(size_t j = 0; j<v.size(); ++j) {
      edges.push_back({k, v[j]});
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
      // table 
      table_lock_map_latch_.lock();
      for(auto& [k, v] : table_lock_map_) {
        std::list<std::shared_ptr<LockRequest>>::iterator iter1 = v->request_queue_.begin();
        std::list<std::shared_ptr<LockRequest>>::iterator iter2 = iter1;
        for(; iter1 != v->request_queue_.end(); ++iter1) {
          if((*iter1)->granted_ == false) {
            for(; iter2 != v->request_queue_.end(); ++iter2) {
              if((*iter2)->granted_ == true) {
                if(!CheckCompatibility((*iter1)->lock_mode_, (*iter2)->lock_mode_)) {
                  AddEdge((*iter1)->txn_id_, (*iter2)->txn_id_);
                }
              }
            }
          }
        }
      }
      table_lock_map_latch_.unlock();
      // row
      row_lock_map_latch_.lock();
      for(auto& [k, v] : row_lock_map_) {
        std::list<std::shared_ptr<LockRequest>>::iterator iter1 = v->request_queue_.begin();
        std::list<std::shared_ptr<LockRequest>>::iterator iter2 = iter1;
        for(; iter1 != v->request_queue_.end(); ++iter1) {
          if((*iter1)->granted_ == false) {
            for(; iter2 != v->request_queue_.end(); ++iter2) {
              if((*iter2)->granted_ == true) {
                if(!CheckCompatibility((*iter1)->lock_mode_, (*iter2)->lock_mode_)) {
                  AddEdge((*iter1)->txn_id_, (*iter2)->txn_id_);
                }
              }
            }
          }
        }
      }
      row_lock_map_latch_.unlock();

      for(auto& it : waits_for_) {
        std::sort(it.second.begin(), it.second.end());
      }
      // 检测  TODO: 抛出abort异常？？
      txn_id_t txn_id = -1;
      while(HasCycle(&txn_id)) {
        LOG_INFO("txn : %d", txn_id);
        assert(txn_id != -1);
        auto txn = TransactionManager::GetTransaction(txn_id);
        txn->SetState(TransactionState::ABORTED);
        
        // 删除指定txn_id
        for(auto& [k, v] : table_lock_map_) {
          std::unique_lock<std::mutex> lock(v->latch_);
          std::list<std::shared_ptr<LockRequest>>::iterator iter = v->request_queue_.begin();
          while(iter != v->request_queue_.end()) {
            if((*iter)->granted_ == txn_id) {
             
              iter = v->request_queue_.erase(iter);
              
            }
            else {
              iter++;
            }
          }
          v->cv_.notify_all();
        }
     
        for(auto& [k, v] : row_lock_map_) {
          std::unique_lock<std::mutex> lock(v->latch_);
          std::list<std::shared_ptr<LockRequest>>::iterator iter = v->request_queue_.begin();
          while(iter != v->request_queue_.end()) {
            if((*iter)->granted_ == txn_id) {
              iter = v->request_queue_.erase(iter);
            }
            else {
              iter++;
            }
          }
          v->cv_.notify_all();
        }
        
        // 删除边
        waits_for_.erase(txn_id);
        std::vector<txn_id_t> keys;
        for(auto& it : waits_for_) {
          keys.push_back(it.first);
        }
        for(auto& key : keys) {
          RemoveEdge(key, txn_id);
        }
      }
      
      // 删除
      waits_for_.clear();
      waits_for_latch_.unlock();
    }
  }
}

}  // namespace bustub
