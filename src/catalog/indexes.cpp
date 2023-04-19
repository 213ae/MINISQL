#include "catalog/indexes.h"

#include <utility>

IndexMetadata *IndexMetadata::Create(const index_id_t index_id, const string &index_name,
                                     const table_id_t table_id, const vector<uint32_t> &key_map,
                                     MemHeap *heap) {
  void *buf = heap->Allocate(sizeof(IndexMetadata));
  return new(buf)IndexMetadata(index_id, index_name, table_id, key_map);
}

uint32_t IndexMetadata::SerializeTo(char *buf) const {
  uint32_t  offset = 0;
  MACH_WRITE_UINT32(buf + offset, INDEX_METADATA_MAGIC_NUM);
  offset += sizeof(uint32_t);
  MACH_WRITE_TO(index_id_t , buf + offset, index_id_);
  offset += sizeof(index_id_t);
  MACH_WRITE_UINT32(buf + offset, index_name_.length());
  offset += sizeof(uint32_t);
  MACH_WRITE_STRING(buf + offset, index_name_);
  offset += index_name_.length();
  MACH_WRITE_TO(table_id_t, buf + offset, table_id_);
  offset += sizeof(table_id_t);
  MACH_WRITE_UINT32(buf + offset, key_map_.size());
  offset += sizeof(uint32_t);
  for(auto n : key_map_){
    MACH_WRITE_UINT32(buf + offset, n);
    offset += sizeof(uint32_t);
  }
  return offset;
}

uint32_t IndexMetadata::GetSerializedSize() const {
  return sizeof(uint32_t) +
         sizeof(index_id_t) +
         sizeof(uint32_t) + index_name_.length() +
         sizeof(table_id_t) +
         sizeof(uint32_t) + key_map_.size() * sizeof(uint32_t);
}

uint32_t IndexMetadata::DeserializeFrom(char *buf, IndexMetadata *&index_meta, MemHeap *heap) {
  uint32_t  offset = 0;
  //uint32_t magic_num = MACH_READ_UINT32(buf + offset);
  offset += sizeof(uint32_t);
  index_id_t index_id = MACH_READ_FROM(index_id_t, buf + offset);
  offset += sizeof(index_id_t);
  uint32_t string_len = MACH_READ_UINT32(buf + offset);
  offset += sizeof(uint32_t);
  std::string index_name(buf + offset, buf + offset + string_len);
  offset += string_len;
  table_id_t table_id = MACH_READ_FROM(table_id_t, buf + offset);
  offset += sizeof(table_id_t);
  uint32_t key_map_size = MACH_READ_UINT32(buf + offset);
  offset += sizeof(uint32_t);
  vector<uint32_t> key_map;
  for(uint32_t i = 0; i < key_map_size; i++){
    key_map.emplace_back(MACH_READ_UINT32(buf + offset));
    offset += sizeof(uint32_t);
  }
  index_meta = IndexMetadata::Create(index_id, index_name, table_id, key_map, heap);
  return offset;
}

IndexMetadata::IndexMetadata(const index_id_t index_id, std::string index_name, const table_id_t table_id,
                             std::vector<uint32_t> key_map) : index_id_(index_id), index_name_(std::move(index_name)), table_id_(table_id), key_map_(std::move(key_map)) {}

Index *IndexInfo::CreateIndex(BufferPoolManager *buffer_pool_manager, const string& index_type) {
  size_t max_size = 0;
  for(auto col : key_schema_->GetColumns()){
    max_size += col->GetLength();
  }

  if (index_type == "bptree") {
    if (max_size <= 8) max_size = 16;
    else if (max_size <= 24) max_size = 32;
    else if (max_size <= 56) max_size = 64;
    else if (max_size <= 120) max_size = 128;
    else if (max_size <= 248) max_size = 256;
    else {
      LOG(ERROR) << "GenericKey size is too large";
      return nullptr;
    }
  }else{ return nullptr; }
  return ALLOC_P(heap_, BPlusTreeIndex)(meta_data_->index_id_, key_schema_, max_size, buffer_pool_manager);
}

void IndexInfo::Init(IndexMetadata *meta_data, TableInfo *table_info, BufferPoolManager *buffer_pool_manager, const string& index_type) {
  // Step1: init index metadata and table info
  meta_data_ = meta_data;
  table_info_ = table_info;
  // Step2: mapping index key to key schema
  key_schema_ = Schema::ShallowCopySchema(table_info_->GetSchema(), meta_data_->key_map_, heap_);
  // Step3: call CreateIndex to create the index
  index_ = CreateIndex(buffer_pool_manager, index_type);
}