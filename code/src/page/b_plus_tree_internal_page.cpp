#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/b_plus_tree_internal_page.h"
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
void BPlusTreeInternalPage::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetLSN();
  SetSize(0);
  SetMaxSize(max_size);
  SetParentPageId(parent_id);
  SetPageId(page_id);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey BPlusTreeInternalPage::KeyAt(int index) const {
  return array_[index].first;
}

void BPlusTreeInternalPage::SetKeyAt(int index, const GenericKey *key) {
  array_[index].first = key;
  ASSERT(array_[index].first == key,"Something wrong.");
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 * 未找到则返回-1
 */
int BPlusTreeInternalPage::ValueIndex(const RowId &value) const {
  for (int i = 0; i < GetSize(); ++i) {
    if(array_[i].second == value) return i;
  }
  return -1;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
RowId BPlusTreeInternalPage::ValueAt(int index) const {
  return array_[index].second;
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
RowId BPlusTreeInternalPage::Lookup(const GenericKey *key, const KeyProcessor &KP) const {
  if(KP(array_[1].first, key) > 0) return array_[0].second;
  int start = 1;
  int end = GetSize() - 1;
  while (start <= end) {
    int mid = start + (end - start) / 2;
    if (mid < GetSize() - 1) {
      if (KP(array_[mid].first, key) < 0 && KP(array_[mid + 1].first, key) <= 0) {
        start = mid + 1;
      } else if (KP(array_[mid].first, key) > 0) {
        end = mid - 1;
      } else {
        return array_[mid].second;
      }
    } else {
      return array_[end].second;
    }
  }
  LOG(FATAL) << "Something wrong in BPlusTreeInternalPage::Lookup";
  return -1;
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
void BPlusTreeInternalPage::PopulateNewRoot(const RowId &old_value, const GenericKey *new_key,
                                                     const RowId &new_value) {
  array_[0].second = old_value;
  array_[1].first = new_key;
  array_[1].second = new_value;
  SetSize(2);
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
int BPlusTreeInternalPage::InsertNodeAfter(const RowId &old_value, const GenericKey *new_key,
                                                    const RowId &new_value) {
  int pos = ValueIndex(old_value) + 1;
  //GenericKey last_key = KeyAt(GetSize() - 1);//测试用
  //RowId last_value = ValueAt(GetSize() - 1);//测试用
  for (int i = GetSize(); i >  pos; --i) {
    array_[i] = array_[i - 1];
  }
  array_[pos].first = new_key;
  array_[pos].second = new_value;
  IncreaseSize(1);
  //ASSERT(last_key == KeyAt(GetSize() - 1) && last_value == ValueAt(GetSize() - 1), "Something wrong in memmove");//测试用
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 * buffer_pool_manager 是干嘛的？传给CopyNFrom()用于Fetch数据页
 */
void BPlusTreeInternalPage::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
  int move_node_num = GetSize() - GetSize() / 2;
  recipient->CopyNFrom(&array_[GetSize() / 2], move_node_num, buffer_pool_manager);
  recipient->SetSize(move_node_num);
  SetSize(GetSize() / 2);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 *
 */
void BPlusTreeInternalPage::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  for (int i = 0; i < size ; ++i) {
    array_[GetSize() + i] = items[i];
  }
  for (auto i = 0; i < size; i++) {
    auto *child_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(array_[GetSize() + i].second));
    child_page->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(array_[GetSize() + i].second, true);
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
void BPlusTreeInternalPage::Remove(int index) {
  //GenericKey last_key = KeyAt(GetSize() - 1);//测试用
  //RowId last_value = ValueAt(GetSize() - 1);//测试用
  for (int i = index; i < GetSize() - 1; ++i) {
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
  //ASSERT(last_key == KeyAt(GetSize() - 1) && last_value == ValueAt(GetSize() - 1), "Something wrong in memmove");//测试用
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
RowId BPlusTreeInternalPage::RemoveAndReturnOnlyChild() {
  RowId val{array_[0].second};
  SetPageType(IndexPageType::INVALID_INDEX_PAGE);
  return val;
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
void BPlusTreeInternalPage::MoveAllTo(BPlusTreeInternalPage *recipient, const GenericKey *middle_key,
                                               BufferPoolManager *buffer_pool_manager) {
  array_[0].first = middle_key;
  recipient->CopyNFrom(array_, GetSize(), buffer_pool_manager);
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
void BPlusTreeInternalPage::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const GenericKey *middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {
  MappingType pair(middle_key, array_[0].second);
  for (int i = 0; i < GetSize() - 1; ++i) {
    array_[i] = array_[i + 1];
  }
  recipient->CopyLastFrom(pair, buffer_pool_manager);
  IncreaseSize(-1);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void BPlusTreeInternalPage::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  array_[GetSize()] = pair;
  auto *child_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(pair.second));
  child_page->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(pair.second, true);
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
void BPlusTreeInternalPage::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const GenericKey *middle_key,
                                                       BufferPoolManager *buffer_pool_manager) {
  MappingType pair = array_[GetSize() - 1];
  //memset(&array_[GetSize() - 1], 0, sizeof(MappingType));
  recipient->SetKeyAt(0, middle_key);
  recipient->CopyFirstFrom(pair, buffer_pool_manager);
  IncreaseSize(-1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void BPlusTreeInternalPage::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  for (int i = GetSize(); i >  0; --i) {
    array_[i] = array_[i - 1];
  }
  array_[0].second = pair.second;
  auto *child_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(pair.second));
  child_page->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(pair.second, true);
  IncreaseSize(1);
}

template
class BPlusTreeInternalPage<int, int, BasicComparator<int>>;

template
class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;

template
class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;

template
class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;

template
class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;

template
class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;

template
class BPlusTreeInternalPage<GenericKey<128>, page_id_t, GenericComparator<128>>;

template
class BPlusTreeInternalPage<GenericKey<256>, page_id_t, GenericComparator<256>>;