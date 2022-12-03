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
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  // TODO(students): remove this line after you have implemented the buffer pool manager
  //throw NotImplementedException(
  //    "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //    "exception line in `buffer_pool_manager_instance.cpp`.");
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  frame_id_t frame_id;
  if(!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  }
  else {
    if(!replacer_->Evict(&frame_id)) {
      page_id = nullptr;
      return nullptr;
    }
  }

  *page_id = AllocatePage();
  Page *page = pages_ + frame_id;

  if(page->IsDirty()) {
    disk_manager_->WritePage(page->GetPageId(), page->GetData());
  }
  page_table_->Remove(page->GetPageId());

  page->ResetMemory();
  page->page_id_ = *page_id;
  ++page->pin_count_;
  page_table_->Insert(*page_id, frame_id);
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  return page; 
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * { 
  frame_id_t frame_id;
  if(!page_table_->Find(page_id, frame_id)) {
    if(!free_list_.empty()) {
      frame_id = free_list_.front();
      free_list_.pop_front();
    }
    else {
      if(!replacer_->Evict(&frame_id)) {
        return nullptr;
      }
    }
  }

  Page *page = pages_ + frame_id;
  if(page->IsDirty()) {
    disk_manager_->WritePage(page->GetPageId(), page->GetData());
  }
  page_table_->Remove(page->GetPageId());

  page->ResetMemory();
  page->page_id_ = page_id;
  ++page->pin_count_;
  disk_manager_->ReadPage(page_id, page->data_);

  page_table_->Insert(page_id, frame_id);
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  return page; 
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool { 
  frame_id_t frame_id;
  if(!page_table_->Find(page_id, frame_id))
    return false;
  
  Page *page = pages_ + frame_id;
  if(page->GetPinCount() <= 0)
    return false;
  
  --page->pin_count_;
  if(page->GetPinCount() == 0) {
    replacer_->SetEvictable(frame_id, true);
  }

  page->is_dirty_ = is_dirty;

  return true; 
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool { 
  frame_id_t frame_id;
  if(!page_table_->Find(page_id, frame_id)) { return false; }

  Page *page = pages_ + frame_id;
  disk_manager_->WritePage(page_id, page->GetData());
  page->is_dirty_ = false;

  return true; 
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  for(size_t i = 0; i<pool_size_; ++i) {
    Page *page = pages_ + i;
    if(page->GetPageId() != INVALID_PAGE_ID) {
      disk_manager_->WritePage(page->GetPageId(), page->GetData());
    }
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool { 
  frame_id_t frame_id;
  if(!page_table_->Find(page_id, frame_id))
    return true;
  
  Page *page = pages_ + frame_id;
  if(page->GetPinCount() > 0) {
    return false;
  }

  page_table_->Remove(page_id);
  replacer_->Remove(frame_id);
  free_list_.push_back(frame_id);
  //DeallocatePage();

  return true; 
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
