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

// 找到驱逐帧，只需要在evicatable中找
auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  ++current_timestamp_;
  size_t old_timestamp = 0;
  bool result = false;
  auto iter = list_.begin();
  for (; iter != list_.end(); ++iter) {
    if (is_evictable_[*iter] != 0) {
      if (hast_[*iter].size() < k_) {
        *frame_id = *iter;
        result = true;
        break;
      }
      size_t tmp = current_timestamp_ - hast_[*iter].back();
      if (tmp > old_timestamp) {
        *frame_id = *iter;
        old_timestamp = tmp;
        result = true;
      }
    }
  }

  if (result) {
    hast_.erase(*frame_id);
    list_.remove(*frame_id);
    is_evictable_.erase(*frame_id);
  }
  return result;
}

// 在所有队列中找，然后判断is_evictable
// 找不到要替换时，在is_evictable中找
void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);

  ++current_timestamp_;
  // LOG_INFO("RecordAccess: %d", frame_id);
  auto it = hast_.find(frame_id);
  if (it != hast_.end()) {
    it->second.push_front(current_timestamp_);
    if (it->second.size() > k_) {
      it->second.pop_back();
    }
  } else {
    if (list_.size() == replacer_size_) {
      size_t old_timestamp = 0;
      bool result = false;
      frame_id_t replace_frame;
      auto iter = list_.begin();
      for (; iter != list_.end(); ++iter) {
        if (is_evictable_[*iter] != 0) {
          if (hast_[*iter].size() < k_) {
            replace_frame = *iter;
            result = true;
            break;
          }
          size_t tmp = current_timestamp_ - hast_[*iter].back();
          if (tmp > old_timestamp) {
            replace_frame = *iter;
            old_timestamp = tmp;
            result = true;
          }
        }
      }

      if (result) {
        hast_.erase(replace_frame);
        list_.remove(replace_frame);
        is_evictable_.erase(replace_frame);
      }
    }

    list_.push_back(frame_id);
    hast_[frame_id].push_front(current_timestamp_);
    is_evictable_[frame_id] = 0;
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  ++current_timestamp_;
  // LOG_INFO("SetEvictable: %d", frame_id);
  auto it = hast_.find(frame_id);
  if (it != hast_.end()) {
    if (set_evictable) {
      is_evictable_[frame_id] = 1;
    } else {
      is_evictable_[frame_id] = 0;
    }
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  ++current_timestamp_;
  // LOG_INFO("REMOVE: %d", frame_id);
  auto it = hast_.find(frame_id);
  if (it != hast_.end()) {
    if (is_evictable_.count(frame_id) == 0U) {
      abort();
    }
    list_.remove(frame_id);
    hast_.erase(frame_id);
    is_evictable_.erase(frame_id);
  }
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);

  return is_evictable_.size();
}

}  // namespace bustub
