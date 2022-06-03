#include <algorithm>
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/b_plus_tree_leaf_page.h"

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 * 未初始化next_page_id
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetLSN();
  SetSize(0);
  SetMaxSize(max_size);
  SetParentPageId(parent_id);
  SetPageId(page_id);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const {
  return next_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
  if(next_page_id == 0) {
    LOG(INFO) << "Fatal error";
  }
}

/**
 * Helper method to find the first index i so that array_[i].first >= key
 * NOTE: This method is only used when generating index iterator
 * 二分查找
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const {
  if(comparator(array_[0].first, key) >= 0) return 0;
  int start = 1;
  int end = GetSize() - 1;
  while (start <= end) {
    int mid = start + (end - start) / 2;
    if (comparator(array_[mid].first, key) < 0) {
      start = mid + 1;
    } else if (comparator(array_[mid].first, key) > 0 && comparator(array_[mid - 1].first, key) >= 0) {
      end = mid - 1;
    } else {
      return mid;
    }
  }
  //LOG(FATAL) << "Something wrong in B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex";
  return -1;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  return array_[index].first;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
  return array_[index];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
  int index = 0;
  if(GetSize() == 0 || comparator(array_[0].first, key) > 0) {
    index = 0;
  }else if (comparator(array_[0].first, key) == 0){
    return -1;
  }else if(comparator(array_[GetSize() - 1].first, key) < 0) {
      index = GetSize();
  }else {
    int start = 1;
    int end = GetSize() - 1;
    while (start <= end) {
      int mid = start + (end - start) / 2;
      if (comparator(array_[mid].first, key) < 0) {
        start = mid + 1;
      } else if (comparator(array_[mid].first, key) > 0 && comparator(array_[mid - 1].first, key) >= 0) {
        end = mid - 1;
      } else if (comparator(array_[mid].first, key) > 0 && comparator(array_[mid - 1].first, key) < 0) {
        index = mid;
        break;
      } else {
        return -1;//相等的情况
      }
    }
  }
  //memmove(&array_[index + 1], &array_[index], (GetSize() - index) * sizeof(MappingType));
  for (int i = GetSize(); i > index; --i) {
    array_[i] = array_[i - 1];
  }
  array_[index].first = key;
  array_[index].second = value;
  IncreaseSize(1);
  //LOG(INFO) << GetSize();
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient) {
  int move_node_num = GetSize() - GetSize() / 2;
  recipient->CopyNFrom(&array_[GetSize() / 2], move_node_num);
  recipient->SetSize(move_node_num);
  SetSize(GetSize() / 2);
  //memset(&array_[GetSize() / 2], 0, move_node_num * sizeof(MappingType));
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, int size) {
  //memcpy(&array_[GetSize()], items, size * sizeof(MappingType));
  for (int i = 0; i < size ; ++i) {
    array_[GetSize() + i] = items[i];
  }
  IncreaseSize(size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType &value, const KeyComparator &comparator) const {
  int start = 0;
  int end = GetSize() - 1;
  while (start <= end) {
    int mid = start + (end - start) / 2;
    if (comparator(array_[mid].first, key) < 0) {
      start = mid + 1;
    } else if (comparator(array_[mid].first, key) > 0) {
      end = mid - 1;
    } else {
      value = array_[mid].second;
      return true;
    }
  }
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return  page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) {
  int start = 0;
  int end = GetSize() - 1;
  while (start <= end) {
    int mid = start + (end - start) / 2;
    if (comparator(array_[mid].first, key) < 0) {
      start = mid + 1;
    } else if (comparator(array_[mid].first, key) > 0) {
      end = mid - 1;
    } else {
      //(&array_[mid], &array_[mid + 1], (GetSize() - mid) * sizeof(MappingType));
      for (int i = mid; i < GetSize() - 1; ++i) {
        array_[i] = array_[i + 1];
      }
      IncreaseSize(-1);
      return GetSize();
    }
  }
  return -1;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient) {
  recipient->CopyNFrom(array_, GetSize());
  SetPageType(IndexPageType::INVALID_INDEX_PAGE);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 *
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient) {
  MappingType pair = array_[0];
  //memmove(array_, &array_[1], (GetSize() - 1) * sizeof(MappingType));
  for (int i = 0; i < GetSize() - 1; ++i) {
    array_[i] = array_[i + 1];
  }
  recipient->CopyLastFrom(pair);
  IncreaseSize(-1);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
  array_[GetSize()] = item;
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient) {
  MappingType pair = array_[GetSize() - 1];
  //memset(&array_[GetSize() - 1], 0, sizeof(MappingType));
  recipient->CopyFirstFrom(pair);
  IncreaseSize(-1);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 *
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item) {
  //memmove(&array_[1], array_, GetSize() * sizeof(MappingType));
  for (int i = GetSize(); i >  0; --i) {
    array_[i] = array_[i - 1];
  }
  array_[0] = item;
  IncreaseSize(1);
}

template
class BPlusTreeLeafPage<int, int, BasicComparator<int>>;

template
class BPlusTreeLeafPage<GenericKey<4>, RowId, GenericComparator<4>>;

template
class BPlusTreeLeafPage<GenericKey<8>, RowId, GenericComparator<8>>;

template
class BPlusTreeLeafPage<GenericKey<16>, RowId, GenericComparator<16>>;

template
class BPlusTreeLeafPage<GenericKey<32>, RowId, GenericComparator<32>>;

template
class BPlusTreeLeafPage<GenericKey<64>, RowId, GenericComparator<64>>;

template
class BPlusTreeLeafPage<GenericKey<128>, RowId, GenericComparator<128>>;

template
class BPlusTreeLeafPage<GenericKey<256>, RowId, GenericComparator<256>>;