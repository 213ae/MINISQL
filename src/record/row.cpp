#include "record/row.h"
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  if(GetFieldCount() == 0) return 0;
  uint32_t offset = 0;
  MACH_WRITE_UINT32(buf + offset, GetFieldCount());
  offset += sizeof(uint32_t);
  uint8_t bitmap[GetFieldCount() / 8 + 1];
  for (size_t i = 0; i < GetFieldCount() / 8 + 1; ++i) {
    bitmap[i] = 0;
    for (int j = 0; j < 8; ++j) {
      if (i * 8 + j == GetFieldCount()) break;
      if (GetField(i * 8 + j)->IsNull()) {
        bitmap[i] |= 0x80 >> j;
      }
    }
    MACH_WRITE_TO(uint8_t, buf + offset, bitmap[i]);
    offset += sizeof(uint8_t);
  }
  if (GetFieldCount() == schema->GetColumnCount()) {
    for (auto field : fields_) {
      offset += field->SerializeTo(buf + offset);
    }
  }else{
    LOG(WARNING) << "Illegal row.";
  }
  return offset;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  uint32_t offset = 0;
  uint32_t field_num = MACH_READ_UINT32(buf + offset);
  offset += sizeof(uint32_t);
  uint8_t bitmap_buf;
  bool bitmap[(field_num / 8 + 1) * 8];
  memcpy(bitmap, buf + offset, (field_num / 8 + 1) * sizeof (uint8_t));
  for (uint32_t i = 0; i < field_num / 8 + 1; ++i) {
    bitmap_buf = MACH_READ_FROM(uint8_t, buf + offset);
    for (int j = 0; j < 8; ++j) {
      bitmap[i * 8 + j] = ((0x80 >> j) & bitmap_buf) != 0;
    }
  }
  offset += (field_num / 8 + 1) * sizeof (uint8_t);
  if (field_num == schema->GetColumnCount()) {
    for (uint32_t i = 0; i < field_num; i++){
      if(fields_.size() < field_num) fields_.push_back(nullptr);
      offset += Field::DeserializeFrom(buf + offset, schema->GetColumn(i)->GetType(), &fields_[i], bitmap[i], heap_);
    }
  }else{
    LOG(WARNING) << "Illegal row.";
  }
  return offset;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  if(GetFieldCount() == 0) return 0;
  uint32_t serialized_size = sizeof(uint32_t);
  serialized_size += (GetFieldCount() / 8 + 1) * sizeof(uint8_t);
  if (GetFieldCount() == schema->GetColumnCount()) {
    for (auto field : fields_) {
      serialized_size += field->GetSerializedSize();
    }
  }else{
    LOG(WARNING) << "Illegal row.";
  }
  return serialized_size;
}
