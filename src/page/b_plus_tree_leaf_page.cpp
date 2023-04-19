#include <algorithm>
#include "index/generic_key.h"
#include "page/b_plus_tree_leaf_page.h"

#define pairs_off (data_ + LEAF_PAGE_HEADER_SIZE)
#define pair_size (GetKeySize() + sizeof(RowId))
#define key_off 0
#define val_off GetKeySize()
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 * 未初始化next_page_id
 */
void LeafPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetLSN();
  SetSize(0);
  SetKeySize(key_size);
  if (max_size == UNDEFINED_SIZE)
    max_size = (int)((PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / (key_size + sizeof(RowId)) - 1);
  SetMaxSize(max_size);
  SetParentPageId(parent_id);
  SetPageId(page_id);
}

/**
 * Helper methods to set/get next page id
 */
page_id_t LeafPage::GetNextPageId() const {
  return next_page_id_;
}

void LeafPage::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
  if(next_page_id == 0) {
    LOG(INFO) << "Fatal error";
  }
}

/**
 * Helper method to find the first index i so that pairs_[i].first >= key
 * NOTE: This method is only used when generating index iterator
 * 二分查找
 */
int LeafPage::KeyIndex(const GenericKey *key, const KeyManager &KM) {
  if(KM.CompareKeys(KeyAt(0), key) >= 0) return 0;
  int start = 1;
  int end = GetSize() - 1;
  while (start <= end) {
    int mid = start + (end - start) / 2;
    if (KM.CompareKeys(KeyAt(mid), key) < 0) {
      start = mid + 1;
    } else if (KM.CompareKeys(KeyAt(mid), key) > 0 && KM.CompareKeys(KeyAt(mid - 1), key) >= 0) {
      end = mid - 1;
    } else {
      return mid;
    }
  }
  //LOG(FATAL) << "Something wrong in LeafPage::KeyIndex";
  return -1;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey* LeafPage::KeyAt(int index) {
//  switch (GetKeySize()) {
//    case 16:
//      return &KV_array_.k16[index].first;
//    case 32:
//      return &KV_array_.k32[index].first;
//    case 64:
//      return &KV_array_.k64[index].first;
//    case 128:
//      return &KV_array_.k128[index].first;
//    case 256:
//      return &KV_array_.k256[index].first;
//  }
//  return nullptr;
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void LeafPage::SetKeyAt(int index, GenericKey *key) {
//  switch (GetKeySize()) {
//    case 16:
//      KV_array_.k16[index].first = *reinterpret_cast<Key16 *>(key);
//      break;
//    case 32:
//      KV_array_.k32[index].first = *reinterpret_cast<Key32 *>(key);
//      break;
//    case 64:
//      KV_array_.k64[index].first = *reinterpret_cast<Key64 *>(key);
//      break;
//    case 128:
//      KV_array_.k128[index].first = *reinterpret_cast<Key128 *>(key);
//      break;
//    case 256:
//      KV_array_.k256[index].first = *reinterpret_cast<Key256 *>(key);
//      break;
//  }
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

RowId LeafPage::ValueAt(int index) const {
//  switch (GetKeySize()) {
//    case 16:
//      return KV_array_.k16[index].second;
//    case 32:
//      return KV_array_.k32[index].second;
//    case 64:
//      return KV_array_.k64[index].second;
//    case 128:
//      return KV_array_.k128[index].second;
//    case 256:
//      return KV_array_.k256[index].second;
//  }
//  return INVALID_PAGE_ID;
  return *reinterpret_cast<const RowId *>(pairs_off + index * pair_size + val_off);
}

void LeafPage::SetValueAt(int index, RowId value) {
//  switch (GetKeySize()) {
//    case 16:
//      KV_array_.k16[index].second = value;
//      break;
//    case 32:
//      KV_array_.k32[index].second = value;
//      break;
//    case 64:
//      KV_array_.k64[index].second = value;
//      break;
//    case 128:
//      KV_array_.k128[index].second = value;
//      break;
//    case 256:
//      KV_array_.k256[index].second = value;
//      break;
//  }
  *reinterpret_cast<RowId *>(pairs_off + index * pair_size + val_off) = value;
}

void *LeafPage::PairPtrAt(int index) { return KeyAt(index); }

void LeafPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(RowId)));
}
/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a. array offset)
 */
std::pair<GenericKey *, RowId> LeafPage::GetItem(int index) {
  return {KeyAt(index), ValueAt(index)};
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return page size after insertion
 */
int LeafPage::Insert(GenericKey *key, const RowId &value, const KeyManager &KM) {
  int index = 0;
  if(GetSize() == 0 || KM.CompareKeys(KeyAt(0), key) > 0) {
    index = 0;
  }else if (KM.CompareKeys(KeyAt(0), key) == 0){
    return -1;
  }else if(KM.CompareKeys(KeyAt(GetSize() - 1), key) < 0) {
      index = GetSize();
  }else {
    int start = 1;
    int end = GetSize() - 1;
    while (start <= end) {
      int mid = start + (end - start) / 2;
      if (KM.CompareKeys(KeyAt(mid), key) < 0) {
        start = mid + 1;
      } else if (KM.CompareKeys(KeyAt(mid), key) > 0 && KM.CompareKeys(KeyAt(mid - 1), key) >= 0) {
        end = mid - 1;
      } else if (KM.CompareKeys(KeyAt(mid), key) > 0 && KM.CompareKeys(KeyAt(mid - 1), key) < 0) {
        index = mid;
        break;
      } else {
        return -1;//相等的情况
      }
    }
  }
  //memmove(&pairs_[index + 1], &pairs_[index], (GetSize() - index) * sizeof(MappingType));
  for (int i = GetSize(); i > index; --i) {
    PairCopy(PairPtrAt(i), PairPtrAt(i - 1));
  }
  IncreaseSize(1);
  SetKeyAt(index, key);
  SetValueAt(index,value);
  //LOG(INFO) << GetSize();
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
void LeafPage::MoveHalfTo(LeafPage *recipient) {
  int move_node_num = GetSize() - GetSize() / 2;
  recipient->CopyNFrom(PairPtrAt(GetSize() / 2), move_node_num);
  recipient->SetSize(move_node_num);
  SetSize(GetSize() / 2);
  //memset(&pairs_[GetSize() / 2], 0, move_node_num * sizeof(MappingType));
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
void LeafPage::CopyNFrom(void *src, int size) {
  //memcpy(&pairs_[GetSize()], items, size * sizeof(MappingType));
  PairCopy(PairPtrAt(GetSize()), src, size);
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
bool LeafPage::Lookup(const GenericKey *key, RowId &value, const KeyManager &KM) {
  int start = 0;
  int end = GetSize() - 1;
  while (start <= end) {
    int mid = start + (end - start) / 2;
    if (KM.CompareKeys(KeyAt(mid), key) < 0) {
      start = mid + 1;
    } else if (KM.CompareKeys(KeyAt(mid), key) > 0) {
      end = mid - 1;
    } else {
      value = ValueAt(mid);
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
 * existed, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return  page size after deletion
 */
int LeafPage::RemoveAndDeleteRecord(const GenericKey *key, const KeyManager &KM) {
  int start = 0;
  int end = GetSize() - 1;
  while (start <= end) {
    int mid = start + (end - start) / 2;
    if (KM.CompareKeys(KeyAt(mid), key) < 0) {
      start = mid + 1;
    } else if (KM.CompareKeys(KeyAt(mid), key) > 0) {
      end = mid - 1;
    } else {
      //(&pairs_[mid], &pairs_[mid + 1], (GetSize() - mid) * sizeof(MappingType));
      for (int i = mid; i < GetSize() - 1; ++i) {
        PairCopy(PairPtrAt(i), PairPtrAt(i + 1));
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
 * Remove all key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
void LeafPage::MoveAllTo(LeafPage *recipient) {
  recipient->CopyNFrom(PairPtrAt(0), GetSize());
  SetPageType(IndexPageType::INVALID_INDEX_PAGE);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 *
 */
void LeafPage::MoveFirstToEndOf(LeafPage *recipient) {
  //memmove(pairs_, &pairs_[1], (GetSize() - 1) * sizeof(MappingType));
  for (int i = 0; i < GetSize() - 1; ++i) {
    PairCopy(PairPtrAt(i), PairPtrAt(i + 1));
  }
  recipient->CopyLastFrom(KeyAt(0), ValueAt(0));
  IncreaseSize(-1);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
void LeafPage::CopyLastFrom(GenericKey *key, const RowId value) {
  SetKeyAt(GetSize(),key);
  SetValueAt(GetSize(),value);
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
void LeafPage::MoveLastToFrontOf(LeafPage *recipient) {
  //memset(&pairs_[GetSize() - 1], 0, sizeof(MappingType));
  recipient->CopyFirstFrom(KeyAt(GetSize() - 1), ValueAt(GetSize() - 1));
  IncreaseSize(-1);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 *
 */
void LeafPage::CopyFirstFrom(GenericKey *key, const RowId value) {
  //memmove(&pairs_[1], pairs_, GetSize() * sizeof(MappingType));
  for (int i = GetSize(); i >  0; --i) {
    PairCopy(PairPtrAt(i), PairPtrAt(i - 1));
  }
  SetKeyAt(0,key);
  SetValueAt(0,value);
  IncreaseSize(1);
}