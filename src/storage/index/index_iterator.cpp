/**
 * index_iterator.cpp
 */
#include <cassert>

#include "common/logger.h"
#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *buffer_pool_manager, LeafPage *leaf, int index)
    : buffer_pool_manager_(buffer_pool_manager), iter_(leaf), index_(index) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return iter_ == nullptr; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { return iter_->GetKeyValue(index_); }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  ++index_;
  if (index_ >= iter_->GetSize()) {
    if (iter_->GetNextPageId() != INVALID_PAGE_ID) {
      auto *new_iter = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(iter_->GetNextPageId())->GetData());
      buffer_pool_manager_->UnpinPage(iter_->GetPageId(), true);
      iter_ = new_iter;
      index_ = 0;
    } else {
      buffer_pool_manager_->UnpinPage(iter_->GetPageId(), true);
      iter_ = nullptr;
      index_ = 0;
    }
  }

  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
