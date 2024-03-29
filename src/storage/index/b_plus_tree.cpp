#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeaf(const KeyType &key) -> LeafPage * {
  auto *page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
  assert(page != nullptr);
  while (!page->IsLeafPage()) {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    // int ids = internal->FindKey(key, comparator_);
    int ids = 1;
    for (; ids < internal->GetSize(); ++ids) {
      if (comparator_(internal->KeyAt(ids), key) > 0) {
        break;
      }
    }
    page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(internal->ValueAt(ids - 1))->GetData());
    buffer_pool_manager_->UnpinPage(internal->GetPageId(), false);
  }

  return reinterpret_cast<LeafPage *>(page);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * compare: k1 < k2 -1
 *          k1 = k2 0
 *          k1 > k2 1
 * This method is used for point query
 * @return : true means key exists
 */

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  LOG_INFO("GetValue");
  if (IsEmpty()) {
    return false;
  }

  // 找到叶子结点
  auto *leaf = FindLeaf(key);
  // int ids = leaf->FindKey(key, comparator_);
  assert(leaf != nullptr);
  for (int ids = 0; ids < leaf->GetSize(); ++ids) {
    if (comparator_(leaf->KeyAt(ids), key) == 0) {
      result->push_back(leaf->ValueAt(ids));
    }
  }

  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
  return !result->empty();
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInParent(BPlusTreePage *left, BPlusTreePage *right, const KeyType &key) {
  LOG_INFO("InsertInParent");
  if (left->IsRootPage()) {
    auto *parent_internal = reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(&root_page_id_)->GetData());
    parent_internal->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
    parent_internal->SetValueAt(0, left->GetPageId());
    parent_internal->PushKey(key, right->GetPageId(), comparator_);
    left->SetParentPageId(root_page_id_);
    right->SetParentPageId(root_page_id_);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    buffer_pool_manager_->UnpinPage(left->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(right->GetPageId(), true);
    UpdateRootPageId(false);
    return;
  }

  auto *curr_internal =
      reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(left->GetParentPageId())->GetData());

  if (curr_internal->GetSize() >= curr_internal->GetMaxSize()) {
    int size = curr_internal->GetSize() + 1;
    std::pair<KeyType, int> tmp_kvs[size];
    std::memset(tmp_kvs, 0, sizeof tmp_kvs);
    tmp_kvs[0].second = curr_internal->ValueAt(0);
    int k = 1;
    for (int i = 1; i < curr_internal->GetSize(); ++i, ++k) {
      tmp_kvs[k].first = curr_internal->KeyAt(i);
      tmp_kvs[k].second = curr_internal->ValueAt(i);
      if (curr_internal->ValueAt(i) == left->GetPageId()) {
        ++k;
        tmp_kvs[k].first = key;
        tmp_kvs[k].second = right->GetPageId();
      }
    }

    int mid = size / 2;
    curr_internal->SetValueAt(0, tmp_kvs[0].second);
    for (int i = 1; i < mid; ++i) {
      curr_internal->SetKeyAt(i, tmp_kvs[i].first);
      curr_internal->SetValueAt(i, tmp_kvs[i].second);
    }
    curr_internal->SetSize(mid);
    page_id_t right_page_id;
    auto *right_internal = reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(&right_page_id)->GetData());
    right_internal->Init(right_page_id, curr_internal->GetParentPageId(), internal_max_size_);
    right_internal->SetValueAt(0, tmp_kvs[mid].second);
    for (int right_id = mid + 1; right_id < size; ++right_id) {
      right_internal->PushKey(tmp_kvs[right_id].first, tmp_kvs[right_id].second, comparator_);
    }
    for (int i = 0; i < right_internal->GetSize(); ++i) {
      auto *child =
          reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(right_internal->ValueAt(i))->GetData());
      child->SetParentPageId(right_page_id);
      buffer_pool_manager_->UnpinPage(child->GetPageId(), true);
    }

    buffer_pool_manager_->UnpinPage(left->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(right->GetPageId(), true);
    InsertInParent(curr_internal, right_internal, tmp_kvs[mid].first);
  } else {
    curr_internal->PushKey(key, right->GetPageId(), comparator_);
    buffer_pool_manager_->UnpinPage(left->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(right->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(curr_internal->GetPageId(), true);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  latch_.lock();
  LOG_INFO("Insert");
  // 空树插入
  if (IsEmpty()) {
    auto *leaf = reinterpret_cast<LeafPage *>(buffer_pool_manager_->NewPage(&root_page_id_)->GetData());
    leaf->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
    leaf->PushKey(key, value, comparator_);
    UpdateRootPageId(true);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    latch_.unlock();
    return true;
  }

  // 找到叶子结点插入
  auto *leaf = FindLeaf(key);
  assert(leaf != nullptr);
  if (!leaf->PushKey(key, value, comparator_)) {
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
    latch_.unlock();
    return false;
  }

  // 分裂
  if (leaf->GetSize() == leaf->GetMaxSize()) {
    int mid = leaf->GetSize() / 2;

    page_id_t right_page_id;
    auto *rightleaf = reinterpret_cast<LeafPage *>(buffer_pool_manager_->NewPage(&right_page_id)->GetData());
    rightleaf->Init(right_page_id, leaf->GetParentPageId(), leaf_max_size_);
    rightleaf->SetNextPageId(leaf->GetNextPageId());
    leaf->SetNextPageId(right_page_id);
    for (int right_id = mid; right_id < leaf->GetMaxSize(); ++right_id) {
      rightleaf->PushKey(leaf->KeyAt(right_id), leaf->ValueAt(right_id), comparator_);
    }
    leaf->SetSize(mid);
    InsertInParent(leaf, rightleaf, leaf->KeyAt(mid));
  } else {
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
  }
  latch_.unlock();
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveInParent(InternalPage *iter, Transaction *transaction) {
  LOG_INFO("RemoveInParent");
  if (iter->IsRootPage()) {
    if (iter->GetSize() == 1) {
      auto *child = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(iter->ValueAt(0))->GetData());
      buffer_pool_manager_->UnpinPage(iter->GetPageId(), true);
      buffer_pool_manager_->DeletePage(iter->GetPageId());
      transaction->AddIntoDeletedPageSet(iter->GetPageId());
      child->SetParentPageId(INVALID_PAGE_ID);
      root_page_id_ = child->GetPageId();
      UpdateRootPageId(false);
    }
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    return;
  }

  if (iter->GetSize() - 1 < iter->GetMinSize()) {
    auto *parent =
        reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(iter->GetParentPageId())->GetData());
    int vi = 0;
    for (; vi < parent->GetSize(); ++vi) {
      if (parent->ValueAt(vi) == iter->GetPageId()) {
        break;
      }
    }

    InternalPage *left_bro =
        vi > 0 ? reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent->ValueAt(vi - 1))->GetData())
               : nullptr;

    // 左兄弟有富余
    if (left_bro != nullptr && left_bro->GetSize() - 1 > left_bro->GetMinSize()) {
      int size = left_bro->GetSize();
      for (int i = iter->GetSize() - 1; i >= 0; --i) {
        iter->SetValueAt(i + 1, iter->ValueAt(i));
      }
      iter->SetValueAt(0, left_bro->ValueAt(size - 1));
      auto *child =
          reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(left_bro->ValueAt(size - 1))->GetData());
      child->SetParentPageId(iter->GetPageId());
      buffer_pool_manager_->UnpinPage(child->GetPageId(), true);
      // 修改key
      iter->SetKeyAt(1, parent->KeyAt(vi));
      iter->IncreaseSize(1);
      // Draw(buffer_pool_manager_, "/home/cgBustub/build/my-tree1.dot");
      parent->SetKeyAt(vi, left_bro->KeyAt(size - 1));
      left_bro->IncreaseSize(-1);
      // Draw(buffer_pool_manager_, "/home/cgBustub/build/my-tree1.dot");
      buffer_pool_manager_->UnpinPage(iter->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(left_bro->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
      return;
    }

    InternalPage *right_bro =
        vi < parent->GetSize() - 1
            ? reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent->ValueAt(vi + 1))->GetData())
            : nullptr;
    // 右兄弟有富余
    if (right_bro != nullptr && right_bro->GetSize() - 1 > right_bro->GetMinSize()) {
      iter->PushKey(parent->KeyAt(vi + 1), right_bro->ValueAt(0), comparator_);
      auto *child =
          reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(right_bro->ValueAt(0))->GetData());
      child->SetParentPageId(iter->GetPageId());
      buffer_pool_manager_->UnpinPage(child->GetPageId(), true);
      // Draw(buffer_pool_manager_, "/home/cgBustub/build/my-tree1.dot");
      // 修改key
      parent->SetKeyAt(vi + 1, right_bro->KeyAt(1));
      // Draw(buffer_pool_manager_, "/home/cgBustub/build/my-tree1.dot");
      right_bro->SetValueAt(0, right_bro->ValueAt(1));
      // Draw(buffer_pool_manager_, "/home/cgBustub/build/my-tree1.dot");
      right_bro->PushForward();
      buffer_pool_manager_->UnpinPage(iter->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(right_bro->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
      if (left_bro != nullptr) {
        buffer_pool_manager_->UnpinPage(left_bro->GetPageId(), false);
      }
      return;
    }

    // 左右都不能借，父节点key下移，合并
    // 合并左兄弟
    if (left_bro != nullptr) {
      left_bro->PushKey(parent->KeyAt(vi), iter->ValueAt(0), comparator_);
      auto *child = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(iter->ValueAt(0))->GetData());
      child->SetParentPageId(left_bro->GetPageId());
      buffer_pool_manager_->UnpinPage(child->GetPageId(), true);
      // Draw(buffer_pool_manager_, "/home/cgBustub/build/my-tree1.dot");
      for (int i = 1; i < iter->GetSize(); ++i) {
        left_bro->PushKey(iter->KeyAt(i), iter->ValueAt(i), comparator_);
        auto *child = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(iter->ValueAt(i))->GetData());
        child->SetParentPageId(left_bro->GetPageId());
        buffer_pool_manager_->UnpinPage(child->GetPageId(), true);
      }
      // Draw(buffer_pool_manager_, "/home/cgBustub/build/my-tree1.dot");
      parent->DeleteWithValue(iter->GetPageId());
      // Draw(buffer_pool_manager_, "/home/cgBustub/build/my-tree1.dot");
      buffer_pool_manager_->UnpinPage(left_bro->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(iter->GetPageId(), true);
      buffer_pool_manager_->DeletePage(iter->GetPageId());
      transaction->AddIntoDeletedPageSet(iter->GetPageId());
      if (right_bro != nullptr) {
        buffer_pool_manager_->UnpinPage(right_bro->GetPageId(), false);
      }
    } else if (right_bro != nullptr) {  // 合并右兄弟
      iter->PushKey(parent->KeyAt(vi + 1), right_bro->ValueAt(0), comparator_);
      auto *child =
          reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(right_bro->ValueAt(0))->GetData());
      child->SetParentPageId(iter->GetPageId());
      buffer_pool_manager_->UnpinPage(child->GetPageId(), true);
      // Draw(buffer_pool_manager_, "/home/cgBustub/build/my-tree1.dot");
      for (int i = 1; i < right_bro->GetSize(); ++i) {
        iter->PushKey(right_bro->KeyAt(i), right_bro->ValueAt(i), comparator_);
        auto *child =
            reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(right_bro->ValueAt(i))->GetData());
        child->SetParentPageId(iter->GetPageId());
        buffer_pool_manager_->UnpinPage(child->GetPageId(), true);
      }
      // Draw(buffer_pool_manager_, "/home/cgBustub/build/my-tree1.dot");
      parent->DeleteWithValue(right_bro->GetPageId());
      // Draw(buffer_pool_manager_, "/home/cgBustub/build/my-tree1.dot");
      buffer_pool_manager_->UnpinPage(iter->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(right_bro->GetPageId(), true);
      buffer_pool_manager_->DeletePage(right_bro->GetPageId());
      transaction->AddIntoDeletedPageSet(right_bro->GetPageId());
    }
    RemoveInParent(parent, transaction);
  } else {
    buffer_pool_manager_->UnpinPage(iter->GetPageId(), true);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  LOG_INFO("Remove");
  latch_.lock();

  if (IsEmpty()) {
    latch_.unlock();
    return;
  }

  // 找到所在叶子结点
  auto *leaf = FindLeaf(key);
  int index = leaf->DeleteKey(key, comparator_);
  if (index == -1) {
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
    latch_.unlock();
    return;
  }

  // 如果叶子结点为根结点,即b+树只有一个节点
  if (leaf->IsRootPage()) {
    if (leaf->GetSize() == 0) {
      // UpdateRootPageId(true);
      buffer_pool_manager_->UnpinPage(root_page_id_, true);
      buffer_pool_manager_->DeletePage(root_page_id_);
      transaction->AddIntoDeletedPageSet(root_page_id_);
      root_page_id_ = INVALID_PAGE_ID;
    } else {
      buffer_pool_manager_->UnpinPage(root_page_id_, true);
    }
    latch_.unlock();
    return;
  }

  // 先删除，然后再判断
  // 如果需要合并，则父节点删除key，并一直向上判断是否需要借或合并
  if (leaf->GetSize() < leaf->GetMinSize()) {
    auto *parent =
        reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(leaf->GetParentPageId())->GetData());

    int vi = 0;
    for (; vi < parent->GetSize(); ++vi) {
      if (parent->ValueAt(vi) == leaf->GetPageId()) {
        break;
      }
    }

    LeafPage *left_bro =
        vi > 0 ? reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(parent->ValueAt(vi - 1))->GetData())
               : nullptr;
    // 成功借左兄弟
    if (left_bro != nullptr && left_bro->GetSize() > left_bro->GetMinSize()) {
      int size = left_bro->GetSize();
      leaf->PushKey(left_bro->KeyAt(size - 1), left_bro->ValueAt(size - 1), comparator_);
      // Draw(buffer_pool_manager_, "/home/cgBustub/build/my-tree1.dot");
      left_bro->IncreaseSize(-1);
      // 修改祖先结点key
      parent->SetKeyAt(vi, leaf->KeyAt(0));
      // Draw(buffer_pool_manager_, "/home/cgBustub/build/my-tree1.dot");
      buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(left_bro->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
      latch_.unlock();
      return;
    }

    LeafPage *right_bro =
        vi < parent->GetSize() - 1
            ? reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(parent->ValueAt(vi + 1))->GetData())
            : nullptr;
    // 成功借右兄弟
    if (right_bro != nullptr && right_bro->GetSize() > right_bro->GetMinSize()) {
      leaf->PushKey(right_bro->KeyAt(0), right_bro->ValueAt(0), comparator_);
      right_bro->DeleteKey(right_bro->KeyAt(0), comparator_);
      // 修改祖先结点key
      parent->SetKeyAt(vi + 1, right_bro->KeyAt(0));
      // Draw(buffer_pool_manager_, "/home/cgBustub/build/my-tree1.dot");
      buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(right_bro->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
      if (left_bro != nullptr) {
        buffer_pool_manager_->UnpinPage(left_bro->GetPageId(), false);
      }
      latch_.unlock();
      return;
    }

    // 左右都不能借，删除合并
    // 合并左兄弟
    if (left_bro != nullptr) {
      left_bro->SetNextPageId(leaf->GetNextPageId());
      for (int i = 0; i < leaf->GetSize(); ++i) {
        left_bro->PushKey(leaf->KeyAt(i), leaf->ValueAt(i), comparator_);
      }
      buffer_pool_manager_->UnpinPage(left_bro->GetPageId(), true);
      // 删除leafpage
      parent->DeleteWithValue(leaf->GetPageId());
      buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
      buffer_pool_manager_->DeletePage(leaf->GetPageId());
      transaction->AddIntoDeletedPageSet(leaf->GetPageId());
      if (right_bro != nullptr) {
        buffer_pool_manager_->UnpinPage(right_bro->GetPageId(), false);
      }
    } else if (right_bro != nullptr) {  // 合并右兄弟
      leaf->SetNextPageId(right_bro->GetNextPageId());
      for (int i = 0; i < right_bro->GetSize(); ++i) {
        leaf->PushKey(right_bro->KeyAt(i), right_bro->ValueAt(i), comparator_);
      }
      buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
      parent->DeleteWithValue(right_bro->GetPageId());
      buffer_pool_manager_->UnpinPage(right_bro->GetPageId(), true);
      buffer_pool_manager_->DeletePage(right_bro->GetPageId());
      transaction->AddIntoDeletedPageSet(right_bro->GetPageId());
    }
    // 处理祖先结点，直到根结点
    RemoveInParent(parent, transaction);
  } else {
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
  }

  latch_.unlock();
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return INDEXITERATOR_TYPE(buffer_pool_manager_, nullptr, 0);
  }
  auto *page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
  while (!page->IsLeafPage()) {
    auto internal = reinterpret_cast<InternalPage *>(page);
    page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(internal->ValueAt(0))->GetData());
    buffer_pool_manager_->UnpinPage(internal->GetPageId(), false);
  }

  return INDEXITERATOR_TYPE(buffer_pool_manager_, reinterpret_cast<LeafPage *>(page), 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return INDEXITERATOR_TYPE(buffer_pool_manager_, nullptr, 0);
  }
  auto *leaf = FindLeaf(key);
  int index = 0;
  for (; index < leaf->GetSize(); ++index) {
    if (comparator_(leaf->KeyAt(index), key) == 0) {
      break;
    }
  }
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf, index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(buffer_pool_manager_, nullptr, 0); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return root_page_id_;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
