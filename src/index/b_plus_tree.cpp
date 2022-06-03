#include <string>
#include "glog/logging.h"
#include "index/b_plus_tree.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
        : index_id_(index_id),
          buffer_pool_manager_(buffer_pool_manager),
          comparator_(comparator),
          leaf_max_size_(leaf_max_size),
          internal_max_size_(internal_max_size) {
  auto root_page = reinterpret_cast<IndexRootsPage*>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID));
  root_page->GetRootId(index_id_, &root_page_id_);
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, false);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Destroy(page_id_t current_page_id) {
  if(current_page_id == INVALID_PAGE_ID) {
    auto root_page = reinterpret_cast<IndexRootsPage*>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID));
    root_page->Delete(index_id_);
    buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
    if(root_page_id_ != INVALID_PAGE_ID) Destroy(root_page_id_);
  }else{
    auto current_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(current_page_id));
    if (!current_page->IsLeafPage()) {
      for (int i = 0; i < current_page->GetSize(); ++i) {
        Destroy(current_page->ValueAt(i));
      }
    }
    buffer_pool_manager_->UnpinPage(current_page_id, false);
    buffer_pool_manager_->DeletePage(current_page_id);
  }
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const {
  if(root_page_id_ == INVALID_PAGE_ID) return true;
  auto root_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_));
  buffer_pool_manager_->UnpinPage(root_page_id_, false);
  if(root_page->GetSize() == 0) return true;
  else return false;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> &result, Transaction *transaction) {
  if(IsEmpty()) return false;
  auto leaf_page = reinterpret_cast<LeafPage *>(FindLeafPage(key));
  ValueType rid;
  if(leaf_page->Lookup(key, rid, comparator_)){
    result.push_back(rid);
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return true;
  }else{
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return false;
  }
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
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  if(IsEmpty()){
    StartNewTree(key, value);
    return true;
  }else{
    return InsertIntoLeaf(key, value, transaction);
  }
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  auto root_page = reinterpret_cast<LeafPage *>(buffer_pool_manager_->NewPage(root_page_id_));
  if(root_page == nullptr) LOG(FATAL) << "Out of memory.";
  UpdateRootPageId(1);
  root_page->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
  root_page->SetNextPageId(INVALID_PAGE_ID);
  root_page->Insert(key, value, comparator_);
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  auto leaf_page = reinterpret_cast<LeafPage *>(FindLeafPage(key));
  if(leaf_page->Insert(key, value, comparator_) == -1) {
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return false;
  }
  if(leaf_page->GetSize() > leaf_max_size_){
    Split(leaf_page, transaction);
  }
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *BPLUSTREE_TYPE::Split(InternalPage *node, Transaction *transaction) {
  page_id_t new_page_id;
  auto* new_page = reinterpret_cast<InternalPage*>(buffer_pool_manager_->NewPage(new_page_id));
  new_page->Init(new_page_id, node->GetParentPageId(), node->GetMaxSize());
  KeyType key_pass_to_parent = node->KeyAt(node->GetSize() / 2);
  node->MoveHalfTo(new_page, buffer_pool_manager_);//能不能根据模板类型的不同调用正确的方法
  buffer_pool_manager_->UnpinPage(new_page_id, true);
  InsertIntoParent(node, key_pass_to_parent, new_page, transaction);
  return new_page;
}

INDEX_TEMPLATE_ARGUMENTS
BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *BPLUSTREE_TYPE::Split(LeafPage *node, Transaction *transaction) {
  page_id_t new_page_id;
  auto* new_page = reinterpret_cast<LeafPage*>(buffer_pool_manager_->NewPage(new_page_id));
  new_page->Init(new_page_id, node->GetParentPageId(), node->GetMaxSize());
  new_page->SetNextPageId(node->GetNextPageId());
  node->SetNextPageId(new_page_id);
  KeyType key_pass_to_parent = node->KeyAt(node->GetSize() / 2);
  node->MoveHalfTo(new_page);//能不能根据模板类型的不同调用正确的方法
  buffer_pool_manager_->UnpinPage(new_page_id, true);
  InsertIntoParent(node, key_pass_to_parent, new_page, transaction);
  return new_page;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  if(old_node->GetParentPageId() == INVALID_PAGE_ID && new_node->GetParentPageId() == INVALID_PAGE_ID){
    auto new_root_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(root_page_id_));
    UpdateRootPageId(0);
    new_root_page->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
    new_root_page->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    old_node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);
  }else if(old_node->GetParentPageId() != INVALID_PAGE_ID && new_node->GetParentPageId() != INVALID_PAGE_ID){
    auto parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(old_node->GetParentPageId()));
    parent_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    buffer_pool_manager_->UnpinPage(old_node->GetParentPageId(), true);
    if(parent_page->GetSize() > parent_page->GetMaxSize()){
      Split(parent_page, transaction);
    }
  }else{
    LOG(FATAL) << "Something wrong.";
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if(IsEmpty()) return;
  auto leaf_page = reinterpret_cast<LeafPage *>(FindLeafPage(key));
  KeyType old_first_key = leaf_page->KeyAt(0);
  int size_after_delete = leaf_page->RemoveAndDeleteRecord(key, comparator_);
  if(size_after_delete != -1){
    KeyType new_first_key = leaf_page->KeyAt(0);
    if(comparator_(old_first_key, new_first_key) != 0 && !leaf_page->IsRootPage()){//检查删完后第一个pair是否发生了改变，如果改变，要更新父节点(如果不是root）
      auto parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(leaf_page->GetParentPageId()));
      int child_index = parent_page->ValueIndex(leaf_page->GetPageId());
      while(child_index == 0){
        page_id_t child_page_id = parent_page->GetPageId();
        page_id_t parent_page_id = parent_page->GetParentPageId();
        buffer_pool_manager_->UnpinPage(child_page_id, false);
        if(parent_page_id != INVALID_PAGE_ID){
          parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_page_id));
        }else{
          break;
        }
        child_index = parent_page->ValueIndex(child_page_id);
      }
      if(parent_page != nullptr && child_index != 0) {
        parent_page->SetKeyAt(child_index, new_first_key);
        buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
      }
    }
    if(!leaf_page->IsRootPage() && size_after_delete < leaf_page->GetMinSize()){
      if(CoalesceOrRedistribute(leaf_page, transaction)){
        buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
        buffer_pool_manager_->DeletePage(leaf_page->GetPageId());
      }else{
        buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
      }
    }
  }else{
    LOG(INFO) << "Nonexistent node.";
  }
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
/*INDEX_TEMPLATE_ARGUMENTS
template<typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {

  return false;
}*/
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N*& node, Transaction *transaction) {
  if(node->IsRootPage()){
    if (node->GetSize() < 2) { if (AdjustRoot(node)) return true; }
    return false;
  }
  auto parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(node->GetParentPageId()));
  int node_index = parent_page->ValueIndex(node->GetPageId());
  if(node_index != 0 && node_index != parent_page->GetSize() - 1){
    auto prev_sibling = reinterpret_cast<N *>(buffer_pool_manager_->FetchPage(parent_page->ValueAt(node_index - 1)));
    auto next_sibling = reinterpret_cast<N *>(buffer_pool_manager_->FetchPage(parent_page->ValueAt(node_index + 1)));
    if(prev_sibling->GetSize() > prev_sibling->GetMinSize()){
      Redistribute(prev_sibling, node, 1);//将上一个兄弟的最后一个pair转移到开头，需更新父节点中的键值
      if (node->IsLeafPage()) {//node是叶结点的情况
        parent_page->SetKeyAt(node_index, node->KeyAt(0));
      } else {//node是内部结点的情况
        auto* temp_page = reinterpret_cast<LeafPage *>(FindLeafPage(KeyType{}, node->GetPageId(), true));
        KeyType key = temp_page->KeyAt(0);
        buffer_pool_manager_->UnpinPage(temp_page->GetPageId(), false);
        parent_page->SetKeyAt(parent_page->ValueIndex(node->GetPageId()), key);
      }
      buffer_pool_manager_->UnpinPage(prev_sibling->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(next_sibling->GetPageId(), false);
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
      return false;
    }else if(next_sibling->GetSize() > next_sibling->GetMinSize()){
      Redistribute(next_sibling, node, 0);//将下一个兄弟的第一个pair转移到末尾，需更新父节点下一个兄弟的键值
      if(node->IsLeafPage()){//node是叶结点的情况
        parent_page->SetKeyAt(parent_page->ValueIndex(next_sibling->GetPageId()), next_sibling->KeyAt(0));
      }else{//node是内部结点的情况
        auto* temp_page = reinterpret_cast<LeafPage *>(FindLeafPage(KeyType{}, next_sibling->GetPageId(), true));
        KeyType key = temp_page->KeyAt(0);
        buffer_pool_manager_->UnpinPage(temp_page->GetPageId(), false);
        parent_page->SetKeyAt(parent_page->ValueIndex(next_sibling->GetPageId()), key);
      }
      buffer_pool_manager_->UnpinPage(prev_sibling->GetPageId(), false);
      buffer_pool_manager_->UnpinPage(next_sibling->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
      return false;
    }else{
      if(Coalesce(prev_sibling, node, parent_page, node_index)){
        if(CoalesceOrRedistribute(parent_page, transaction)){
          buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
          buffer_pool_manager_->DeletePage(parent_page->GetPageId());
        }else{
          buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
        }
      }
      buffer_pool_manager_->UnpinPage(next_sibling->GetPageId(), false);
      buffer_pool_manager_->UnpinPage(prev_sibling->GetPageId(), true);
      return true;
    }
  }else if(node_index == 0){
    auto next_sibling = reinterpret_cast<N *>(buffer_pool_manager_->FetchPage(parent_page->ValueAt(node_index + 1)));
    if(next_sibling->GetSize() > next_sibling->GetMinSize()){
      Redistribute(next_sibling, node, 0);//将下一个兄弟的第一个pair转移到末尾，需更新父节点中下一个兄弟的键值
      if(node->IsLeafPage()){//node是叶结点的情况
        parent_page->SetKeyAt(parent_page->ValueIndex(next_sibling->GetPageId()), next_sibling->KeyAt(0));
      }else{//node是内部结点的情况
        auto* temp_page = reinterpret_cast<LeafPage *>(FindLeafPage(KeyType{}, next_sibling->GetPageId(), true));
        KeyType key = temp_page->KeyAt(0);
        buffer_pool_manager_->UnpinPage(temp_page->GetPageId(), false);
        parent_page->SetKeyAt(parent_page->ValueIndex(next_sibling->GetPageId()), key);
      }
      buffer_pool_manager_->UnpinPage(next_sibling->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
      return false;
    }else{
      if(Coalesce(next_sibling, node, parent_page, node_index)){
        if(CoalesceOrRedistribute(parent_page, transaction)){
          buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
          buffer_pool_manager_->DeletePage(parent_page->GetPageId());
        }else{
          buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
        }
      }
      buffer_pool_manager_->UnpinPage(next_sibling->GetPageId(), true);
      return true;
    }
  }else if(node_index == parent_page->GetSize() - 1){
    auto prev_sibling = reinterpret_cast<N *>(buffer_pool_manager_->FetchPage(parent_page->ValueAt(node_index - 1)));
    if(prev_sibling->GetSize() > prev_sibling->GetMinSize()){
      Redistribute(prev_sibling, node, 1);//将上一个兄弟的最后一个pair转移到开头，需更新父节点中的键值
      if (node->IsLeafPage()) {//node是叶结点的情况
        parent_page->SetKeyAt(node_index, node->KeyAt(0));
      } else {//node是内部结点的情况
        auto* temp_page = reinterpret_cast<LeafPage *>(FindLeafPage(KeyType{}, node->GetPageId(), true));
        KeyType key = temp_page->KeyAt(0);
        buffer_pool_manager_->UnpinPage(temp_page->GetPageId(), false);
        parent_page->SetKeyAt(parent_page->ValueIndex(node->GetPageId()), key);
      }
      buffer_pool_manager_->UnpinPage(prev_sibling->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
      return false;
    }else{
      if(Coalesce(prev_sibling, node, parent_page, node_index)){
        if(CoalesceOrRedistribute(parent_page, transaction)){
          buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
          buffer_pool_manager_->DeletePage(parent_page->GetPageId());
        }else{
          buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
        }
      }
      buffer_pool_manager_->UnpinPage(prev_sibling->GetPageId(), true);
      return true;
    }
  }else{
    LOG(ERROR) << "Something wrong in CoalesceOrRedistribute.";
    return false;
  }
}


/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Coalesce(LeafPage *&neighbor_node, LeafPage *&node, InternalPage *&parent, int index,
                              Transaction *transaction) {
  if(index == 0){
    auto temp_node= node;
    node = neighbor_node;
    neighbor_node = temp_node;
  }
  node->MoveAllTo(neighbor_node);
  neighbor_node->SetNextPageId(node->GetNextPageId());
  parent->Remove(parent->ValueIndex(node->GetPageId()));
  if(parent->GetSize() < parent->GetMinSize()) return true;
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Coalesce(InternalPage *&neighbor_node, InternalPage *&node, InternalPage *&parent, int index,
                              Transaction *transaction) {
  if(index == 0){
    auto temp_node= node;
    node = neighbor_node;
    neighbor_node = temp_node;
  }
  auto* temp_page = reinterpret_cast<LeafPage *>(FindLeafPage(KeyType{}, node->GetPageId(), true));
  KeyType key = temp_page->KeyAt(0);
  buffer_pool_manager_->UnpinPage(temp_page->GetPageId(), false);
  node->MoveAllTo(neighbor_node, key, buffer_pool_manager_);
  parent->Remove(parent->ValueIndex(node->GetPageId()));
  if(parent->GetSize() < parent->GetMinSize()) return true;
  return false;
}


/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Redistribute(LeafPage *neighbor_node, LeafPage *node, int index) {
  if(index == 0){
    neighbor_node->MoveFirstToEndOf(node);
  }else{
    neighbor_node->MoveLastToFrontOf(node);
  }
}
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Redistribute(InternalPage *neighbor_node, InternalPage *node, int index) {
  if(index == 0){
    auto* temp_page = reinterpret_cast<LeafPage *>(FindLeafPage(KeyType{}, neighbor_node->GetPageId(), true));
    KeyType key = temp_page->KeyAt(0);
    buffer_pool_manager_->UnpinPage(temp_page->GetPageId(), false);
    neighbor_node->MoveFirstToEndOf(node, key, buffer_pool_manager_);
  }else{
    auto* temp_page = reinterpret_cast<LeafPage *>(FindLeafPage(KeyType{}, node->GetPageId(), true));
    KeyType key = temp_page->KeyAt(0);
    buffer_pool_manager_->UnpinPage(temp_page->GetPageId(), false);
    neighbor_node->MoveLastToFrontOf(node, key, buffer_pool_manager_);
  }
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  if(old_root_node->GetSize() == 0){
    auto index_root_page = reinterpret_cast<IndexRootsPage*>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID));
    index_root_page->Delete(index_id_);
    buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
    return true;
  }else if(old_root_node->GetSize() == 1){
    root_page_id_ = reinterpret_cast<InternalPage *>(old_root_node)->RemoveAndReturnOnlyChild();
    auto new_root_page = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(root_page_id_));
    new_root_page->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    UpdateRootPageId();
    return true;
  }else{
    LOG(ERROR) << "old_root_node->GetSize() < 0";
    return false;
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() {
  if(IsEmpty()) return End();
  page_id_t page_id = FindLeafPage(KeyType{}, root_page_id_, true)->GetPageId();
  buffer_pool_manager_->UnpinPage(page_id, false);
  return INDEXITERATOR_TYPE(page_id, buffer_pool_manager_);
}


/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  if(IsEmpty()) return End();
  auto leaf_page = reinterpret_cast<LeafPage *>(FindLeafPage(key, root_page_id_, false));
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  int index = leaf_page->KeyIndex(key, comparator_);
  if(index != -1) return INDEXITERATOR_TYPE(leaf_page->GetPageId(), buffer_pool_manager_, leaf_page->KeyIndex(key, comparator_));
  else return End();
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::End() {
  return INDEXITERATOR_TYPE();
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, page_id_t page_id, bool leftMost) {
  if(IsEmpty()) return nullptr;
  page_id_t next_page_id = page_id;
  if(next_page_id == INVALID_PAGE_ID) next_page_id = root_page_id_;
  auto page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(next_page_id));
  if (!leftMost) {
    while (!page->IsLeafPage()) {
      buffer_pool_manager_->UnpinPage(next_page_id, false);
      next_page_id = page->Lookup(key, comparator_);
      page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(next_page_id));
    }
  } else {
    while (!page->IsLeafPage()) {
      buffer_pool_manager_->UnpinPage(next_page_id, false);
      next_page_id = page->ValueAt(0);
      page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(next_page_id));
    }
  }
  return reinterpret_cast<Page*>(page);
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, current_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto index_root_page = reinterpret_cast<IndexRootsPage*>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID));
  if(insert_record != 0) {
    index_root_page->Insert(index_id_, root_page_id_);
  }else{
    index_root_page->Update(index_id_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
}

/**
 * This method is used for debug only, You don't need to modify
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
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
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
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId()
          << "};\n";
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
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
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
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> "
          << internal_prefix
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
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId()
              << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}

template
class BPlusTree<int, int, BasicComparator<int>>;

template
class BPlusTree<GenericKey<4>, RowId, GenericComparator<4>>;

template
class BPlusTree<GenericKey<8>, RowId, GenericComparator<8>>;

template
class BPlusTree<GenericKey<16>, RowId, GenericComparator<16>>;

template
class BPlusTree<GenericKey<32>, RowId, GenericComparator<32>>;

template
class BPlusTree<GenericKey<64>, RowId, GenericComparator<64>>;

template
class BPlusTree<GenericKey<128>, RowId, GenericComparator<128>>;

template
class BPlusTree<GenericKey<256>, RowId, GenericComparator<256>>;