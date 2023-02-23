//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

#include "common/logger.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetPageType(IndexPageType::LEAF_PAGE);
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code

  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::FindKey(const KeyType &key, const KeyComparator &comparator) -> int {
  int l = 0;
  int r = GetSize() - 1;
  while (l <= r) {  // 找到小于等于key的位置
    int mid = l + (r - l) / 2;
    if (comparator(array_[mid].first, key) > 0) {
      r = mid - 1;
    } else {
      l = mid + 1;
    }
  }

  return r;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::PushKey(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
    -> bool {
  /**
  int i = 0;
  int size = GetSize();
  for (; i < size; ++i) {
    if (comparator(KeyAt(i), key) == 0) {
      return false;
    }
    if (comparator(KeyAt(i), key) > 0) {
      break;
    }
  }
  **/

  int size = GetSize();
  int i = FindKey(key, comparator);
  if (i > -1 && comparator(KeyAt(i), key) == 0) {
    return false;
  }

  IncreaseSize(1);

  for (int k = size - 1; k > i; --k) {
    array_[k + 1] = array_[k];
  }

  array_[i + 1] = std::make_pair(key, value);

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::DeleteKey(const KeyType &key, const KeyComparator &comparator) -> int {
  /**
  int i = 0;
  for (; i < GetSize(); ++i) {
    if (comparator(KeyAt(i), key) == 0) {
      break;
    }
  }
  if (i == GetSize()) {
    return -1;
  }
  **/
  int size = GetSize();
  int i = FindKey(key, comparator);

  if (i == -1 || comparator(KeyAt(i), key) != 0) {
    return -1;
  }

  IncreaseSize(-1);

  for (int j = i + 1; j < size; ++j) {
    array_[j - 1] = array_[j];
  }

  return i;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetKeyValue(int index) -> const MappingType & { return array_[index]; }

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
