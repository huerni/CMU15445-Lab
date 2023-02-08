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

  while (!page->IsLeafPage()) {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    buffer_pool_manager_->UnpinPage(internal->GetPageId(), false);
    for (int i = 1; i <= internal->GetSize(); ++i) {
      page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(internal->ValueAt(i - 1))->GetData());
      if (i < internal->GetSize() && comparator_(internal->KeyAt(i), key) > 0) {
        break;
      }
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    }
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
  if (IsEmpty()) {
    return false;
  }

  // 找到叶子结点
  auto *leaf = FindLeaf(key);
  for (int i = 0; i < leaf->GetSize(); ++i) {
    if (comparator_(leaf->KeyAt(i), key) == 0) {
      result->push_back(leaf->ValueAt(i));
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
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  // 空树插入
  if (IsEmpty()) {
    auto *leaf = reinterpret_cast<LeafPage *>(buffer_pool_manager_->NewPage(&root_page_id_)->GetData());
    leaf->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
    leaf->PushKey(key, value, comparator_);
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
    UpdateRootPageId(true);
    return true;
  }

  // 找到叶子结点插入
  auto *result = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
  while (!result->IsLeafPage()) {
    auto *internal = reinterpret_cast<InternalPage *>(result);
    buffer_pool_manager_->UnpinPage(internal->GetPageId(), false);
    for (int i = 1; i <= internal->GetSize(); ++i) {
      result = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(internal->ValueAt(i - 1))->GetData());
      if (i < internal->GetSize() && comparator_(internal->KeyAt(i), key) > 0) {
        break;
      }
      buffer_pool_manager_->UnpinPage(result->GetPageId(), false);
    }
  }

  auto *result_leaf = reinterpret_cast<LeafPage *>(result);
  if (!result_leaf->PushKey(key, value, comparator_)) {
    buffer_pool_manager_->UnpinPage(result->GetPageId(), false);
    return false;
  }

  // 分裂
  while (result->GetSize() == result->GetMaxSize()) {
    int mid = result->GetSize() / 2;

    page_id_t right_page_id;
    auto *right = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->NewPage(&right_page_id)->GetData());

    // 找到parent
    BPlusTreePage *parent = nullptr;
    page_id_t parent_page_id;
    if (result->IsRootPage()) {
      parent = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->NewPage(&parent_page_id)->GetData());
      auto *parent_internal = reinterpret_cast<InternalPage *>(parent);
      root_page_id_ = parent_page_id;
      parent_internal->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
      parent_internal->SetKeyAt(0, result_leaf->KeyAt(0));
      parent_internal->SetValueAt(0, result->GetPageId());
      result->SetParentPageId(parent_page_id);
      UpdateRootPageId(false);
    } else {
      parent_page_id = result->GetParentPageId();
      parent = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(parent_page_id)->GetData());
    }

    // 分裂该结点
    if (result->IsLeafPage()) {
      auto *rightleaf = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(right_page_id)->GetData());
      auto *leftleaf = reinterpret_cast<LeafPage *>(result);
      // error: address points to the zero page.(setPageType) ??
      rightleaf->Init(right_page_id, parent_page_id, leaf_max_size_);
      rightleaf->SetNextPageId(leftleaf->GetNextPageId());
      leftleaf->SetNextPageId(right_page_id);
      for (int right_id = mid; right_id < result->GetMaxSize(); ++right_id) {
        rightleaf->PushKey(leftleaf->KeyAt(right_id), leftleaf->ValueAt(right_id), comparator_);
      }
      buffer_pool_manager_->UnpinPage(rightleaf->GetPageId(), true);
      (reinterpret_cast<InternalPage *>(parent))->PushKey(leftleaf->KeyAt(mid), right_page_id, comparator_);
    } else {
      auto *right_internal = reinterpret_cast<InternalPage *>(right);
      auto *left_internal = reinterpret_cast<InternalPage *>(result);
      right_internal->Init(right_page_id, result->GetParentPageId(), internal_max_size_);
      // right_internal->SetValueAt(0, left_internal->ValueAt(mid));
      for (int right_id = mid; right_id < result->GetMaxSize(); ++right_id) {
        right_internal->PushKey(left_internal->KeyAt(right_id), left_internal->ValueAt(right_id), comparator_);
      }
  
      (reinterpret_cast<InternalPage *>(parent))->PushKey(left_internal->KeyAt(mid), right_page_id, comparator_);
    }

    // 填充新增分裂,将孩子结点父节点指向自己
    if (!right->IsLeafPage()) {
      auto *right_internal = reinterpret_cast<InternalPage *>(right);
      for (int i = 0; i < right_internal->GetSize(); ++i) {
        auto *child =
            reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(right_internal->ValueAt(i))->GetData());
        child->SetParentPageId(right_page_id);
      }
    }

    buffer_pool_manager_->UnpinPage(right->GetPageId(), true);
    result->SetSize(mid);
    buffer_pool_manager_->UnpinPage(result->GetPageId(), true);
    result = parent;
  }

  buffer_pool_manager_->UnpinPage(result->GetPageId(), true);
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
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty()) {
    return;
  }

  // 找到所在叶子结点
  auto *page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
  // page_id_t page_id = INVALID_PAGE_ID;
  while (!page->IsLeafPage()) {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    for (int i = 1; i <= internal->GetSize(); ++i) {
      page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(internal->ValueAt(i - 1))->GetData());
      if (comparator_(internal->KeyAt(i), key) > 0) {
        break;
      }
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    }
  }

  auto *leaf = reinterpret_cast<LeafPage *>(page);

  int index = leaf->DeleteKey(key, comparator_);
  if (index == -1) {
    return;
  }

  // 如果叶子结点为根结点
  if (page->IsRootPage()) {
    if (page->GetSize() == 0) {
      buffer_pool_manager_->DeletePage(root_page_id_);
      root_page_id_ = INVALID_PAGE_ID;
      UpdateRootPageId(true);
    }
    return;
  }

  // 先删除，然后再判断
  // 如果无需合并，则父节点只需要修改
  // 如果需要合并，则父节点删除key，并一直向上判断是否需要借或合并
  if (page->GetSize() >= page->GetMinSize()) {
    // 只需修改，无需借或合并
    if (index == 0) {
      page_id_t page_id = leaf->GetParentPageId();
      AlterKey(page_id, key, leaf->KeyAt(index));
    }
  } else {
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
    LeafPage *right_bro =
        vi < parent->GetSize() - 1
            ? reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(parent->ValueAt(vi + 1))->GetData())
            : nullptr;
    // 成功借左兄弟
    if (left_bro != nullptr && left_bro->GetSize() > left_bro->GetMinSize()) {
      int size = left_bro->GetSize();
      leaf->PushKey(left_bro->KeyAt(size - 1), left_bro->ValueAt(size - 1), comparator_);
      left_bro->DeleteKey(left_bro->KeyAt(size - 1), comparator_);
      // 修改祖先结点key
      AlterKey(leaf->GetParentPageId(), key, leaf->KeyAt(0));
      return;
    }
    // 成功借右兄弟
    if (right_bro != nullptr && right_bro->GetSize() > right_bro->GetMinSize()) {
      leaf->PushKey(right_bro->KeyAt(0), right_bro->ValueAt(0), comparator_);
      KeyType old_key = right_bro->KeyAt(0);
      right_bro->DeleteKey(right_bro->KeyAt(0), comparator_);
      // 修改祖先结点key
      AlterKey(right_bro->GetParentPageId(), old_key, right_bro->KeyAt(0));
      return;
    }

    // 左右都不能借，删除合并
    parent->DeleteKey(leaf->KeyAt(0), comparator_);
    // 合并左兄弟
    if (left_bro != nullptr) {
      if (right_bro != nullptr) {
        left_bro->SetNextPageId(right_bro->GetPageId());
      } else {
        left_bro->SetNextPageId(INVALID_PAGE_ID);
      }
      for (int i = 0; i < leaf->GetSize(); ++i) {
        left_bro->PushKey(leaf->KeyAt(i), leaf->ValueAt(i), comparator_);
      }
    } else if (right_bro != nullptr) {
      KeyType old_key = right_bro->KeyAt(0);
      for (int i = 0; i < leaf->GetSize(); ++i) {
        right_bro->PushKey(leaf->KeyAt(i), leaf->ValueAt(i), comparator_);
      }
      AlterKey(right_bro->GetParentPageId(), old_key, right_bro->KeyAt(0));
    }
    // 删除leafpage
    buffer_pool_manager_->DeletePage(leaf->GetPageId());

    // 处理祖先结点，直到根结点
    auto *iter = parent;
    while (iter->GetSize() < iter->GetMinSize() && !iter->IsRootPage()) {
      parent = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(iter->GetParentPageId())->GetData());
      int vi = 0;
      for (; vi < parent->GetSize(); ++vi) {
        if (parent->ValueAt(vi) == iter->GetPageId()) {
          break;
        }
      }

      InternalPage *left_bro =
          vi > 0 ? reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent->ValueAt(vi - 1))->GetData())
                 : nullptr;
      InternalPage *right_bro =
          vi < parent->GetSize()
              ? reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent->ValueAt(vi + 1))->GetData())
              : nullptr;

      // 左兄弟有富余
      if (left_bro != nullptr && left_bro->GetSize() > left_bro->GetMinSize()) {
        int size = left_bro->GetSize();
        iter->PushKey(parent->KeyAt(vi), left_bro->ValueAt(size - 1), comparator_);
        // 修改祖先结点key
        AlterKey(iter->GetParentPageId(), parent->KeyAt(vi), left_bro->KeyAt(size - 1));
        left_bro->DeleteKey(left_bro->KeyAt(size - 1), comparator_);
        return;
      }
      if (right_bro != nullptr && right_bro->GetSize() > right_bro->GetMinSize()) {
        iter->PushKey(parent->KeyAt(vi), right_bro->ValueAt(0), comparator_);
        right_bro->DeleteKey(right_bro->KeyAt(0), comparator_);
        // 修改祖先结点key
        AlterKey(right_bro->GetParentPageId(), parent->KeyAt(vi), right_bro->KeyAt(0));
        return;
      }

      // 左右都不能借，父节点下移，合并
      // 合并左兄弟
      if (left_bro != nullptr) {
        KeyType old_key = parent->KeyAt(0);
        for (int i = 0; i < left_bro->GetSize(); ++i) {
          parent->PushKey(left_bro->KeyAt(i), left_bro->ValueAt(i), comparator_);
        }
        for (int i = 0; i < iter->GetSize(); ++i) {
          parent->PushKey(iter->KeyAt(i), iter->ValueAt(i), comparator_);
        }
        if (!parent->IsRootPage()) {
          AlterKey(parent->GetParentPageId(), old_key, parent->KeyAt(0));
        }
        buffer_pool_manager_->DeletePage(left_bro->GetPageId());
        buffer_pool_manager_->DeletePage(iter->GetPageId());
      } else if (right_bro != nullptr) {  // 合并右兄弟
        KeyType old_key = parent->KeyAt(0);
        for (int i = 0; i < iter->GetSize(); ++i) {
          parent->PushKey(iter->KeyAt(i), iter->ValueAt(i), comparator_);
        }
        for (int i = 0; i < right_bro->GetSize(); ++i) {
          parent->PushKey(right_bro->KeyAt(i), right_bro->ValueAt(i), comparator_);
        }
        if (!parent->IsRootPage()) {
          AlterKey(parent->GetParentPageId(), old_key, parent->KeyAt(0));
        }
        buffer_pool_manager_->DeletePage(right_bro->GetPageId());
        buffer_pool_manager_->DeletePage(iter->GetPageId());
      }

      iter = parent;
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::AlterKey(page_id_t p_page_id, const KeyType &old_key, const KeyType &new_key) {
  while (p_page_id != INVALID_PAGE_ID) {
    auto *parent = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(p_page_id)->GetData());
    for (int i = 1; i <= parent->GetSize(); ++i) {
      if (comparator_(parent->KeyAt(i), old_key) == 0) {
        parent->SetKeyAt(i, new_key);
        break;
      }
    }
    p_page_id = parent->GetParentPageId();
  }
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
  // if(IsEmpty())
  //  return nullptr;
  auto *page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
  while (!page->IsLeafPage()) {
    auto internal = reinterpret_cast<InternalPage *>(page);
    page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(internal->ValueAt(0))->GetData());
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
  // if(IsEmpty())
  //  return nullptr;
  auto *page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
  while (!page->IsLeafPage()) {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    for (int i = 1; i <= internal->GetSize(); ++i) {
      page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(internal->ValueAt(i - 1))->GetData());
      if (i < internal->GetSize() && comparator_(internal->KeyAt(i), key) > 0) {
        break;
      }
    }
  }

  auto *leaf = reinterpret_cast<LeafPage *>(page);
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
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  auto *page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
  while (!page->IsLeafPage()) {
    auto internal = reinterpret_cast<InternalPage *>(page);
    page = reinterpret_cast<BPlusTreePage *>(
        buffer_pool_manager_->FetchPage(internal->ValueAt(internal->GetSize() - 1))->GetData());
  }

  return INDEXITERATOR_TYPE(buffer_pool_manager_, reinterpret_cast<LeafPage *>(page), page->GetSize());
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

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
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
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
