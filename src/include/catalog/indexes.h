#ifndef MINISQL_INDEXES_H
#define MINISQL_INDEXES_H

#include <memory>

#include "catalog/table.h"
#include "index/generic_key.h"
#include "index/b_plus_tree_index.h"
#include "common/rowid.h"
#include "common/macros.h"
#include "record/schema.h"

class IndexMetadata {
  friend class IndexInfo;

public:
  static IndexMetadata *Create(const index_id_t index_id, const std::string &index_name,
                               const table_id_t table_id, const std::vector<uint32_t> &key_map,
                               MemHeap *heap);

  uint32_t SerializeTo(char *buf) const;

  uint32_t GetSerializedSize() const;

  static uint32_t DeserializeFrom(char *buf, IndexMetadata *&index_meta, MemHeap *heap);

  inline std::string GetIndexName() const { return index_name_; }

  inline table_id_t GetTableId() const { return table_id_; }

  uint32_t GetIndexColumnCount() const { return key_map_.size(); }

  inline const std::vector<uint32_t> &GetKeyMapping() const { return key_map_; }

  inline index_id_t GetIndexId() const { return index_id_; }

private:
  IndexMetadata() = delete;

  explicit IndexMetadata(const index_id_t index_id, const std::string &index_name,
                         const table_id_t table_id, const std::vector<uint32_t> &key_map) ;

private:
  static constexpr uint32_t INDEX_METADATA_MAGIC_NUM = 344528;
  index_id_t index_id_;
  std::string index_name_;
  table_id_t table_id_;
  std::vector<uint32_t> key_map_;  /** The mapping of index key to tuple key */
};

template <size_t N>
using BPIndex = BPlusTreeIndex<GenericKey<N>, RowId, GenericComparator<N>>;

/**
 * The IndexInfo class maintains metadata about a index.
 */
class IndexInfo {
public:
  static IndexInfo *Create(MemHeap *heap) {
    void *buf = heap->Allocate(sizeof(IndexInfo));
    return new(buf)IndexInfo();
  }

  ~IndexInfo() {
    delete heap_;
  }

  void Init(IndexMetadata *meta_data, TableInfo *table_info, BufferPoolManager *buffer_pool_manager) {
    // Step1: init index metadata and table info
    meta_data_ = meta_data;
    table_info_ = table_info;
    // Step2: mapping index key to key schema
    key_schema_ = Schema::ShallowCopySchema(table_info_->GetSchema(), meta_data_->key_map_, heap_);
    // Step3: call CreateIndex to create the index
    index_ = CreateIndex(buffer_pool_manager);
  }

  inline Index *GetIndex() { return index_; }

  inline std::string GetIndexName() { return meta_data_->GetIndexName(); }

  inline IndexSchema *GetIndexKeySchema() { return key_schema_; }

  inline MemHeap *GetMemHeap() const { return heap_; }

  inline TableInfo *GetTableInfo() const { return table_info_; }

private:
  explicit IndexInfo() : meta_data_{nullptr}, index_{nullptr}, table_info_{nullptr},
                         key_schema_{nullptr}, heap_(new SimpleMemHeap()) {}

  Index *CreateIndex(BufferPoolManager *buffer_pool_manager) {
    uint32_t max_size = 8;
    for(auto col : key_schema_->GetColumns()){
      max_size += col->GetLength();
    }
    if(max_size <= 8) return ALLOC_P(heap_, BPIndex<16>)(meta_data_->index_id_, key_schema_, buffer_pool_manager);
    else if(max_size <= 24) return ALLOC_P(heap_, BPIndex<32>)(meta_data_->index_id_, key_schema_, buffer_pool_manager);
    else if(max_size <= 56) return ALLOC_P(heap_, BPIndex<64>)(meta_data_->index_id_, key_schema_, buffer_pool_manager);
    else if(max_size <= 120) return ALLOC_P(heap_, BPIndex<128>)(meta_data_->index_id_, key_schema_, buffer_pool_manager);
    else if(max_size <= 248) return ALLOC_P(heap_, BPIndex<256>)(meta_data_->index_id_, key_schema_, buffer_pool_manager);
    else {
      LOG(ERROR) << "Key size is too large";
      return nullptr;
    }
  }

private:
  IndexMetadata *meta_data_;
  Index *index_;
  TableInfo *table_info_;
  IndexSchema *key_schema_;
  MemHeap *heap_;
};

#endif //MINISQL_INDEXES_H
