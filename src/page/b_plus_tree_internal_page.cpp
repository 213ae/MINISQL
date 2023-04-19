#include "page/b_plus_tree_internal_page.h"
#include "index/generic_key.h"

#define pairs_off (data_ + INTERNAL_PAGE_HEADER_SIZE)
#define pair_size (GetKeySize() + sizeof(page_id_t))
#define key_off 0
#define val_off GetKeySize()

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
void InternalPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetLSN();
  SetSize(0);
  SetKeySize(key_size);
  if (max_size == UNDEFINED_SIZE)
    max_size = (int)((PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (key_size + sizeof(page_id_t)) - 1);
  SetMaxSize(max_size);
  SetParentPageId(parent_id);
  SetPageId(page_id);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *InternalPage::KeyAt(int index) {
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

void InternalPage::SetKeyAt(int index, GenericKey *key) {
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

page_id_t InternalPage::ValueAt(int index) const {
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
  return *reinterpret_cast<const page_id_t *>(pairs_off + index * pair_size + val_off);
}

void InternalPage::SetValueAt(int index, page_id_t value) {
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
  *reinterpret_cast<page_id_t *>(pairs_off + index * pair_size + val_off) = value;
}

int InternalPage::ValueIndex(const page_id_t &value) const {
  for (int i = 0; i < GetSize(); ++i) {
    if (ValueAt(i) == value) return i;
  }
  return -1;
}

void *InternalPage::PairPtrAt(int index) { return KeyAt(index); }

void InternalPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(page_id_t)));
}
/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 * 用了二分查找
 */
page_id_t InternalPage::Lookup(const GenericKey *key, const KeyManager &KM) {
  if (KM.CompareKeys(KeyAt(1), key) > 0) return ValueAt(0);
  int start = 1;
  int end = GetSize() - 1;
  while (start <= end) {
    int mid = start + (end - start) / 2;
    if (mid < GetSize() - 1) {
      if (KM.CompareKeys(KeyAt(mid), key) < 0 && KM.CompareKeys(KeyAt(mid + 1), key) <= 0) {
        start = mid + 1;
      } else if (KM.CompareKeys(KeyAt(mid), key) > 0) {
        end = mid - 1;
      } else {
        return ValueAt(mid);
      }
    } else {
      return ValueAt(end);
    }
  }
  LOG(FATAL) << "Something wrong in InternalPage::Lookup";
  return INVALID_PAGE_ID;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
void InternalPage::PopulateNewRoot(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  SetValueAt(0, old_value);
  SetKeyAt(1, new_key);
  SetValueAt(1, new_value);
  SetSize(2);
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
int InternalPage::InsertNodeAfter(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  int pos = ValueIndex(old_value) + 1;
  // GenericKey last_key = KeyAt(GetSize() - 1);//测试用
  // RowId last_value = ValueAt(GetSize() - 1);//测试用
  for (int i = GetSize(); i > pos; --i) {
    PairCopy(PairPtrAt(i), PairPtrAt(i - 1));  // 也可以提供PairMove不要for循环
  }
  SetKeyAt(pos, new_key);
  SetValueAt(pos, new_value);
  IncreaseSize(1);
  // ASSERT(last_key == KeyAt(GetSize() - 1) && last_value == ValueAt(GetSize() - 1), "Something wrong in
  // memmove");//测试用
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 * buffer_pool_manager 是干嘛的？传给CopyNFrom()用于Fetch数据页
 */
void InternalPage::MoveHalfTo(InternalPage *recipient, BufferPoolManager *buffer_pool_manager) {
  int move_node_num = GetSize() - GetSize() / 2;
  recipient->CopyNFrom(PairPtrAt(GetSize() / 2), move_node_num, buffer_pool_manager);
  recipient->SetSize(move_node_num);
  SetSize(GetSize() / 2);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 *
 */
void InternalPage::CopyNFrom(void *src, int size, BufferPoolManager *buffer_pool_manager) {
  PairCopy(PairPtrAt(GetSize()), src, size);
  for (auto i = 0; i < size; i++) {
    auto *child_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(ValueAt(GetSize() + i)));
    child_page->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(ValueAt(GetSize() + i), true);
  }
  IncreaseSize(size);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
void InternalPage::Remove(int index) {
  // GenericKey last_key = KeyAt(GetSize() - 1);//测试用
  // RowId last_value = ValueAt(GetSize() - 1);//测试用
  for (int i = index; i < GetSize() - 1; ++i) {
    PairCopy(PairPtrAt(i), PairPtrAt(i + 1));
  }
  IncreaseSize(-1);
  // ASSERT(last_key == KeyAt(GetSize() - 1) && last_value == ValueAt(GetSize() - 1), "Something wrong in
  // memmove");//测试用
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
page_id_t InternalPage::RemoveAndReturnOnlyChild() {
  SetPageType(IndexPageType::INVALID_INDEX_PAGE);
  return ValueAt(0);
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveAllTo(InternalPage *recipient, GenericKey *middle_key, BufferPoolManager *buffer_pool_manager) {
  SetKeyAt(0, middle_key);
  recipient->CopyNFrom(PairPtrAt(0), GetSize(), buffer_pool_manager);
  SetPageType(IndexPageType::INVALID_INDEX_PAGE);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveFirstToEndOf(InternalPage *recipient, GenericKey *middle_key,
                                    BufferPoolManager *buffer_pool_manager) {
  for (int i = 0; i < GetSize() - 1; ++i) {
    PairCopy(PairPtrAt(i), PairPtrAt(i + 1));
  }
  recipient->CopyLastFrom(middle_key, ValueAt(0), buffer_pool_manager);
  IncreaseSize(-1);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyLastFrom(GenericKey *key, const page_id_t value, BufferPoolManager *buffer_pool_manager) {
  SetKeyAt(GetSize(), key);
  SetValueAt(GetSize(), value);
  auto *child_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(value));
  child_page->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(value, true);
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
void InternalPage::MoveLastToFrontOf(InternalPage *recipient, GenericKey *middle_key,
                                     BufferPoolManager *buffer_pool_manager) {
  // memset(&pairs_[GetSize() - 1], 0, sizeof(MappingType));
  recipient->SetKeyAt(0, middle_key);
  recipient->CopyFirstFrom(ValueAt(GetSize() - 1), buffer_pool_manager);
  IncreaseSize(-1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyFirstFrom(const page_id_t value, BufferPoolManager *buffer_pool_manager) {
  for (int i = GetSize(); i > 0; --i) {
    PairCopy(PairPtrAt(i), PairPtrAt(i - 1));
  }
  SetValueAt(0, value);
  auto *child_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(value));
  child_page->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(value, true);
  IncreaseSize(1);
}