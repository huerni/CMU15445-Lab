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
  auto iter = list_evictable_.begin();
  for (; iter != list_evictable_.end(); ++iter) {
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

  if (result) {
    hast_.erase(*frame_id);
    list_evictable_.remove(*frame_id);
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
    if (list_.size() + list_evictable_.size() == replacer_size_) {
      size_t old_timestamp = 0;
      bool result = false;
      frame_id_t replace_frame;
      auto iter = list_evictable_.begin();
      for (; iter != list_evictable_.end(); ++iter) {
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

      if (result) {
        hast_.erase(replace_frame);
        list_evictable_.remove(replace_frame);
      }
    }

    list_.push_back(frame_id);
    hast_[frame_id].push_front(current_timestamp_);
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  ++current_timestamp_;
  // LOG_INFO("SetEvictable: %d", frame_id);
  auto it = hast_.find(frame_id);
  if (it != hast_.end()) {
    list_.remove(frame_id);
    list_evictable_.remove(frame_id);
    if (set_evictable) {
      list_evictable_.push_back(frame_id);
    } else {
      list_.push_back(frame_id);
    }
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  ++current_timestamp_;
  // LOG_INFO("REMOVE: %d", frame_id);
  auto it = hast_.find(frame_id);
  if (it != hast_.end()) {
    if (std::find(list_.begin(), list_.end(), frame_id) != list_.end()) {
      abort();
    }
    list_evictable_.remove(frame_id);
    hast_.erase(frame_id);
  }
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);

  return list_evictable_.size();
}

}  // namespace bustub
