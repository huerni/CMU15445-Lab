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
  std::scoped_lock<std::mutex> lock(latch_);
  // LOG_INFO("Evict");
  ++current_timestamp_;
  size_t old_timestamp = 0;
  bool result = false;
  auto iter = list_.begin();
  for (; iter != list_.end(); ++iter) {
    if (iter->is_evictable_) {
      if (iter->hast_.size() < k_) {
        *frame_id = iter->frame_id_;
        result = true;
        break;
      }
      size_t tmp = current_timestamp_ - iter->hast_.back();
      if (tmp > old_timestamp) {
        *frame_id = iter->frame_id_;
        old_timestamp = tmp;
        result = true;
      }
    }
  }

  if (result) {
    list_.erase(cache_[*frame_id]);
    cache_.erase(*frame_id);
    --curr_size_;
  }
  return result;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);

  ++current_timestamp_;
  // LOG_INFO("RecordAccess: %d", frame_id);
  auto it = cache_.find(frame_id);
  if (it != cache_.end()) {
    it->second->hast_.push_front(current_timestamp_);
    if (it->second->hast_.size() > k_) {
      it->second->hast_.pop_back();
    }
  } else {
    if (list_.size() == replacer_size_) {
      size_t old_timestamp = 0;
      frame_id_t replace_frame;
      bool result = false;
      auto iter = list_.begin();
      for (; iter != list_.end(); ++iter) {
        if (iter->is_evictable_) {
          if (iter->hast_.size() < k_) {
            replace_frame = iter->frame_id_;
            result = true;
            break;
          }
          size_t tmp = current_timestamp_ - iter->hast_.back();
          if (tmp > old_timestamp) {
            replace_frame = iter->frame_id_;
            old_timestamp = tmp;
            result = true;
          }
        }
      }

      if (result) {
        list_.erase(cache_[replace_frame]);
        cache_.erase(replace_frame);
      }
    }
    list_.emplace_back(FrameInfo(frame_id, current_timestamp_));
    cache_.emplace(frame_id, std::prev(list_.end()));
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  ++current_timestamp_;
  // LOG_INFO("SetEvictable: %d", frame_id);
  auto it = cache_.find(frame_id);
  if (it != cache_.end()) {
    if (set_evictable && !it->second->is_evictable_) {
      it->second->is_evictable_ = true;
      ++curr_size_;
    } else if (!set_evictable && it->second->is_evictable_) {
      it->second->is_evictable_ = false;
      --curr_size_;
    }
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  ++current_timestamp_;
  // LOG_INFO("REMOVE: %d", frame_id);
  auto it = cache_.find(frame_id);
  if (it != cache_.end()) {
    if (!it->second->is_evictable_) {
      abort();
    }
    list_.erase(cache_[frame_id]);
    cache_.erase(frame_id);
    --curr_size_;
  }
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);

  return curr_size_;
}

}  // namespace bustub
