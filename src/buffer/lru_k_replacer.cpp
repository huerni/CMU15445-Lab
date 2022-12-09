//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  // std::scoped_lock<std::mutex> lock(latch_);
  latch_.lock();
  ++current_timestamp_;
  size_t mmax = 0;
  bool flag = false;
  for (auto &id : lru_) {
    if (is_evictable_.count(id) != 0U) {
      if (hast_[id][k_ - 1] == INT_MAX) {
        *frame_id = id;
        is_evictable_.erase(id);
        cache_.erase(id);
        hast_.erase(id);
        lru_.remove(id);
        // LOG_INFO("Evict: %d", *frame_id);
        latch_.unlock();
        return true;
      }
      size_t tmp = current_timestamp_ - hast_[id][k_ - 1];
      if (tmp > mmax) {
        mmax = tmp;
        *frame_id = id;
        flag = true;
      }
    }
  }

  if (flag) {
    is_evictable_.erase(*frame_id);
    cache_.erase(*frame_id);
    hast_.erase(*frame_id);
    lru_.remove(*frame_id);
    // LOG_INFO("Evict: %d", *frame_id);
  }
  latch_.unlock();
  return flag;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  // std::scoped_lock<std::mutex> lock(latch_);
  latch_.lock();
  ++current_timestamp_;
  // LOG_INFO("RecordAccess: %d", frame_id);
  auto it = cache_.find(frame_id);
  if (it != cache_.end()) {
    // lru_.splice(lru_.end(), lru_, it->second);
    auto vec = hast_[frame_id];
    for (int i = k_ - 1; i > 0; --i) {
      vec[i] = vec[i - 1];
    }
    vec[0] = current_timestamp_;
    hast_[frame_id] = vec;
  } else {
    if (lru_.size() == replacer_size_) {
      size_t mmax = 0;
      bool flag = false;
      frame_id_t pop_id;
      for (auto &id : lru_) {
        if (is_evictable_.count(id) != 0U) {
          if (hast_[id][k_ - 1] == INT_MAX) {
            is_evictable_.erase(id);
            cache_.erase(id);
            hast_.erase(id);
            lru_.remove(id);
          }
          size_t tmp = current_timestamp_ - hast_[id][k_ - 1];
          if (tmp > mmax) {
            mmax = tmp;
            pop_id = id;
            flag = true;
          }
        }
      }

      if (flag) {
        is_evictable_.erase(pop_id);
        cache_.erase(pop_id);
        hast_.erase(pop_id);
        lru_.remove(pop_id);
      }
    }
    lru_.push_back(frame_id);
    cache_.emplace(frame_id, std::prev(lru_.end()));
    std::vector<int> vec(k_, INT_MAX);
    vec[0] = current_timestamp_;
    hast_[frame_id] = vec;
  }
  latch_.unlock();
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  // std::scoped_lock<std::mutex> lock(latch_);
  latch_.lock();
  ++current_timestamp_;
  // LOG_INFO("SetEvictable: %d", frame_id);
  auto it = cache_.find(frame_id);
  if (it == cache_.end()) {
    latch_.unlock();
    return;
  }

  if (set_evictable) {
    is_evictable_.insert(frame_id);
  } else {
    is_evictable_.erase(frame_id);
  }
  latch_.unlock();
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  // std::scoped_lock<std::mutex> lock(latch_);
  latch_.lock();
  ++current_timestamp_;
  // LOG_INFO("REMOVE: %d", frame_id);
  auto it = cache_.find(frame_id);
  if (it == cache_.end()) {
    latch_.unlock();
    return;
  }
  if (is_evictable_.count(frame_id) == 0U) {
    latch_.unlock();
    abort();
  }

  is_evictable_.erase(frame_id);
  cache_.erase(frame_id);
  hast_.erase(frame_id);
  lru_.remove(frame_id);
  latch_.unlock();
}

auto LRUKReplacer::Size() -> size_t { return is_evictable_.size(); }

}  // namespace bustub
