//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

#include "common/logger.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  // LOG_DEBUG("pool_size:%ld, k:%ld", pool_size, replacer_k);
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //    "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //    "exception line in `buffer_pool_manager_instance.cpp`.");
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  // std::scoped_lock<std::mutex> lock(latch_);
  latch_.lock();
  frame_id_t frame_id;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (!replacer_->Evict(&frame_id)) {
    page_id = nullptr;
    latch_.unlock();
    return nullptr;
  }

  *page_id = AllocatePage();
  Page *page = &pages_[frame_id];
  // LOG_DEBUG("NewPgImp, page_id:%d", *page_id);
  if (page->IsDirty()) {
    disk_manager_->WritePage(page->GetPageId(), page->GetData());
    page->is_dirty_ = false;
  }
  page_table_->Remove(page->GetPageId());
  page->ResetMemory();
  page->page_id_ = *page_id;
  page->pin_count_ = 1;
  page_table_->Insert(*page_id, frame_id);
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  latch_.unlock();
  return page;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  // LOG_DEBUG("FetchPgImp, page_id:%d", page_id);
  // std::scoped_lock<std::mutex> lock(latch_);
  latch_.lock();
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    if (!free_list_.empty()) {
      frame_id = free_list_.front();
      free_list_.pop_front();
    } else {
      if (!replacer_->Evict(&frame_id)) {
        latch_.unlock();
        return nullptr;
      }
    }
  } else {
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    Page *page = &pages_[frame_id];
    ++page->pin_count_;
    latch_.unlock();
    return &pages_[frame_id];
  }

  // 没找到，磁盘读取，替换帧
  Page *page = &pages_[frame_id];
  if (page->IsDirty()) {
    disk_manager_->WritePage(page->GetPageId(), page->GetData());
    page->is_dirty_ = false;
  }
  page_table_->Remove(page->GetPageId());

  page->ResetMemory();
  disk_manager_->ReadPage(page_id, page->data_);
  page->page_id_ = page_id;
  page->pin_count_ = 1;

  page_table_->Insert(page_id, frame_id);
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  latch_.unlock();
  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  // std::scoped_lock<std::mutex> lock(latch_);
  // LOG_DEBUG("UnpinPgImp, page_id:%d, is_dirty:%d", page_id, is_dirty);
  latch_.lock();
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    latch_.unlock();
    return false;
  }

  Page *page = &pages_[frame_id];
  if (is_dirty) {
    page->is_dirty_ = true;
  }

  if (page->GetPinCount() <= 0) {
    latch_.unlock();
    return false;
  }

  --page->pin_count_;
  if (page->GetPinCount() == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  latch_.unlock();
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  // LOG_DEBUG("FlushPgImp, page_id:%d", page_id);
  // std::scoped_lock<std::mutex> lock(latch_);
  latch_.lock();
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    latch_.unlock();
    return false;
  }

  Page *page = pages_ + frame_id;
  disk_manager_->WritePage(page_id, page->GetData());
  page->is_dirty_ = false;
  latch_.unlock();
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // std::scoped_lock<std::mutex> lock(latch_);
  // LOG_DEBUG("FlushAllPgsImp");
  latch_.lock();
  for (size_t i = 0; i < pool_size_; ++i) {
    Page *page = pages_ + i;
    if (page->GetPageId() != INVALID_PAGE_ID) {
      disk_manager_->WritePage(page->GetPageId(), page->GetData());
      page->is_dirty_ = false;
    }
  }
  latch_.unlock();
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  // std::scoped_lock<std::mutex> lock(latch_);
  // LOG_DEBUG("DeletePgImp, page_id:%d", page_id);
  latch_.lock();
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    latch_.unlock();
    return true;
  }

  Page *page = &pages_[frame_id];
  if (page->GetPinCount() > 0) {
    latch_.unlock();
    return false;
  }

  page->ResetMemory();
  page_table_->Remove(page_id);
  replacer_->Remove(frame_id);
  free_list_.push_back(frame_id);

  DeallocatePage(page_id);
  latch_.unlock();
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
