#include "catalog/table.h"

#include <utility>

uint32_t TableMetadata::SerializeTo(char *buf) const {
  uint32_t  offset = 0;
  MACH_WRITE_UINT32(buf + offset, TABLE_METADATA_MAGIC_NUM);
  offset += sizeof(uint32_t);
  MACH_WRITE_TO(table_id_t, buf + offset, table_id_);
  offset += sizeof(table_id_t);
  MACH_WRITE_UINT32(buf + offset, table_name_.length());
  offset += sizeof(uint32_t);
  MACH_WRITE_STRING(buf + offset, table_name_);
  offset += table_name_.length();
  MACH_WRITE_TO(page_id_t, buf + offset, root_page_id_);
  offset += sizeof(page_id_t);
  MACH_WRITE_UINT32(buf + offset, primary_key_map.size());
  offset += sizeof(uint32_t);
  for(auto itr : primary_key_map){
    MACH_WRITE_UINT32(buf + offset, itr);
    offset += sizeof(uint32_t);
  }
  offset += schema_->SerializeTo(buf + offset);
  if(offset > PAGE_SIZE) LOG(FATAL) << "table_metadata is too large";
  return offset;
}

uint32_t TableMetadata::GetSerializedSize() const {
  return sizeof(uint32_t) +
         sizeof(table_id_t) +
         sizeof(uint32_t) + sizeof(table_name_.length()) +
         sizeof(page_id_t) +
         sizeof(uint32_t) * (primary_key_map.size() + 1) +
         schema_->GetSerializedSize();
}

/**
 * @param heap Memory heap passed by TableInfo
 */
uint32_t TableMetadata::DeserializeFrom(char *buf, TableMetadata *&table_meta, MemHeap *heap) {
  uint32_t  offset = 0;
  //uint32_t magic_num = MACH_READ_UINT32(buf + offset);
  offset += sizeof(uint32_t);
  table_id_t table_id = MACH_READ_FROM(table_id_t, buf + offset);
  offset += sizeof(table_id_t);
  uint32_t string_len = MACH_READ_UINT32(buf + offset);
  offset += sizeof(uint32_t);
  std::string table_name(buf + offset, buf + offset + string_len);
  offset += string_len;
  page_id_t page_id = MACH_READ_FROM(page_id_t, buf + offset);
  offset += sizeof(page_id_t);
  vector<uint32_t> primary_key_map;
  int size = MACH_READ_UINT32(buf + offset);
  offset += sizeof(uint32_t);
  for(int i = 0; i < size; i++){
    int val = MACH_READ_UINT32(buf + offset);
    primary_key_map.emplace_back(val);
    offset += sizeof(uint32_t);
  }
  Schema *schema;
  offset += Schema::DeserializeFrom(buf + offset, schema, heap);
  table_meta = TableMetadata::Create(table_id, table_name, page_id, schema, heap, primary_key_map);
  return offset;
}

/**
 * Only called by create table
 *
 * @param heap Memory heap passed by TableInfo
 */
TableMetadata *TableMetadata::Create(table_id_t table_id, std::string table_name,
                                     page_id_t root_page_id, TableSchema *schema, MemHeap *heap, vector<uint32_t> primary_key_map) {
  // allocate space for table metadata
  void *buf = heap->Allocate(sizeof(TableMetadata));
  return new(buf)TableMetadata(table_id, std::move(table_name), root_page_id, schema, std::move(primary_key_map));
}

TableMetadata::TableMetadata(table_id_t table_id, std::string table_name, page_id_t root_page_id, TableSchema *schema, vector<uint32_t> primary_key_map)
        : table_id_(table_id), table_name_(std::move(table_name)), root_page_id_(root_page_id), primary_key_map(std::move(primary_key_map)), schema_(schema) {}
