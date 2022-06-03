#include "index/b_plus_tree_index.h"
#include "index/generic_key.h"
#include "utils/tree_file_mgr.h"
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_INDEX_TYPE::BPlusTreeIndex(index_id_t index_id, IndexSchema *key_schema,
                                     BufferPoolManager *buffer_pool_manager)
        : Index(index_id, key_schema),
          comparator_(key_schema_),
          container_(index_id, buffer_pool_manager, comparator_) {

}

INDEX_TEMPLATE_ARGUMENTS
dberr_t BPLUSTREE_INDEX_TYPE::InsertEntry(const Row &key, RowId row_id, Transaction *txn) {
  //ASSERT(row_id.Get() != INVALID_ROWID.Get(), "Invalid row id for index insert.");
  KeyType index_key;
  index_key.SerializeFromKey(key, key_schema_);

  bool status = container_.Insert(index_key, row_id, txn);

  if (!status) {
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

INDEX_TEMPLATE_ARGUMENTS
dberr_t BPLUSTREE_INDEX_TYPE::RemoveEntry(const Row &key, RowId row_id, Transaction *txn) {
  KeyType index_key;
  index_key.SerializeFromKey(key, key_schema_);

  container_.Remove(index_key, txn);
  /*TreeFileManagers mgr("tree_");
  static int i = 0;
  container_.PrintTree(mgr[i++]);*/
  return DB_SUCCESS;
}

INDEX_TEMPLATE_ARGUMENTS
dberr_t BPLUSTREE_INDEX_TYPE::ScanKey(const Row &key, vector<RowId> &result, Transaction *txn, string compare_operator) {
  KeyType index_key;
  index_key.SerializeFromKey(key, key_schema_);
  if (compare_operator == "=") {
    container_.GetValue(index_key, result, txn);
  }else if(compare_operator == ">"){
    auto iter = GetBeginIterator(index_key);
    if(container_.GetValue(index_key, result, txn)) ++iter;
    for(; iter != GetEndIterator(); ++iter){
      result.emplace_back((*iter).second);
    }
  }else if(compare_operator == ">="){
    for(auto iter = GetBeginIterator(index_key); iter != GetEndIterator(); ++iter){
      result.emplace_back((*iter).second);
    }
  }else if(compare_operator == "<"){
    for(auto iter = GetBeginIterator(); iter != GetBeginIterator(index_key); ++iter){
      result.emplace_back((*iter).second);
    }
  }else if(compare_operator == "<="){
    for(auto iter = GetBeginIterator(); iter != GetBeginIterator(index_key); ++iter){
      result.emplace_back((*iter).second);
    }
    container_.GetValue(index_key, result, txn);
  }else if(compare_operator == "<>"){
    for(auto iter = GetBeginIterator(); iter != GetEndIterator(); ++iter){
      result.emplace_back((*iter).second);
    }
    vector<RowId> temp;
    if(container_.GetValue(index_key, temp, txn)) result.erase(find(result.begin(), result.end(), temp[0]));
  }
  if(!result.empty()) return DB_SUCCESS;
  else return DB_KEY_NOT_FOUND;
}

INDEX_TEMPLATE_ARGUMENTS
dberr_t BPLUSTREE_INDEX_TYPE::Destroy() {
  container_.Destroy();
  return DB_SUCCESS;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_INDEX_TYPE::GetBeginIterator() {
  return container_.Begin();
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE
BPLUSTREE_INDEX_TYPE::GetBeginIterator(const KeyType &key) {
  return container_.Begin(key);
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_INDEX_TYPE::GetEndIterator() {
  return container_.End();
}

template
class BPlusTreeIndex<GenericKey<4>, RowId, GenericComparator<4>>;

template
class BPlusTreeIndex<GenericKey<8>, RowId, GenericComparator<8>>;

template
class BPlusTreeIndex<GenericKey<16>, RowId, GenericComparator<16>>;

template
class BPlusTreeIndex<GenericKey<32>, RowId, GenericComparator<32>>;

template
class BPlusTreeIndex<GenericKey<64>, RowId, GenericComparator<64>>;

template
class BPlusTreeIndex<GenericKey<128>, RowId, GenericComparator<128>>;

template
class BPlusTreeIndex<GenericKey<256>, RowId, GenericComparator<256>>;