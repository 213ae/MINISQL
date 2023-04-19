#include "record/schema.h"

uint32_t Schema::SerializeTo(char *buf) const {
  uint32_t offset = 0;
  MACH_WRITE_UINT32(buf + offset, SCHEMA_MAGIC_NUM);
  offset += sizeof(uint32_t);
  MACH_WRITE_UINT32(buf + offset, columns_.size());
  offset += sizeof(uint);
  for(auto column : columns_){
    offset += column->SerializeTo(buf + offset);
  }
  return offset;
}

uint32_t Schema::GetSerializedSize() const {
  uint32_t serialized_size = sizeof(uint32_t);
  for(auto column : columns_){
    serialized_size += column->GetSerializedSize();
  }
  return serialized_size;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema, MemHeap *heap) {
  uint32_t offset = 0;
  //uint32_t magic_num = MACH_READ_UINT32(buf + offset);
  offset += sizeof(uint32_t);
  uint column_num = MACH_READ_FROM(uint, buf + offset);
  offset += sizeof(uint);
  vector<Column *> columns(column_num);
  for(auto &column : columns){
    offset += Column::DeserializeFrom(buf + offset, column, heap);
  }
  schema = ALLOC_P(heap, Schema)(columns);
  return offset;
}