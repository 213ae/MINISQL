#include "record/column.h"
#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(char) * 50;
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

uint32_t Column::SerializeTo(char *buf) const {
  uint32_t buf_offset = 0, column_name_len = name_.size();
  uint8_t flags = unique_ * 2 + nullable_ * 1;
  memcpy(buf + buf_offset, &COLUMN_MAGIC_NUM, sizeof(uint32_t));
  buf_offset += sizeof(uint32_t);
  memcpy(buf + buf_offset, &column_name_len, sizeof(uint32_t));
  buf_offset += sizeof(uint32_t);
  memcpy(buf + buf_offset, name_.c_str(), column_name_len);
  buf_offset += column_name_len;
  memcpy(buf + buf_offset, &type_, sizeof(uint8_t));
  buf_offset += sizeof(uint8_t);
  if (type_ == kTypeChar) {
    memcpy(buf + buf_offset, &len_, sizeof(uint32_t));
    buf_offset += sizeof(uint32_t);
  } else if (type_ != kTypeInt && type_ != kTypeFloat) {
    LOG(WARNING) << "Wrong type.";
    return 0;
  }
  memcpy(buf + buf_offset, &table_ind_, sizeof(uint32_t));
  buf_offset += sizeof(uint32_t);
  memcpy(buf + buf_offset, &flags, sizeof(uint8_t));
  buf_offset += sizeof(uint8_t);
  return buf_offset;
}

uint32_t Column::GetSerializedSize() const {
  if (type_ == kTypeChar) {
    return 4 * sizeof(uint32_t) + 2 * sizeof(uint8_t) + name_.size();
  } else if (type_ == kTypeInt || type_ == kTypeFloat) {
    return 3 * sizeof(uint32_t) + 2 * sizeof(uint8_t) + name_.size();
  } else {
    LOG(WARNING) << "Wrong type.";
    return 0;
  }
}

uint32_t Column::DeserializeFrom(const char *buf, Column *&column, MemHeap *heap) {
  uint32_t offset = 0, len;
  // uint32_t magic_num = MACH_READ_UINT32(buf + offset);
  offset += sizeof(uint32_t);
  uint32_t column_name_len = MACH_READ_UINT32(buf + offset);
  offset += sizeof(uint32_t);
  string column_name(buf + offset, buf + offset + column_name_len);
  offset += column_name_len;
  uint8_t type = MACH_READ_FROM(uint8_t, buf + offset);
  offset += sizeof(uint8_t);
  if (type == kTypeChar) {
    len = MACH_READ_UINT32(buf + offset);
    offset += sizeof(uint32_t);
  } else if (type != kTypeFloat && type != kTypeInt) {
    LOG(FATAL) << "Wrong type.";
  }
  uint32_t col_ind = MACH_READ_UINT32(buf + offset);
  offset += sizeof(uint32_t);
  uint8_t flags = MACH_READ_FROM(uint8_t, buf + offset);
  offset += sizeof(uint8_t);
  bool nullable = flags % 2;
  bool unique = (flags >> 1) % 2;
  if (type == kTypeChar) {
    column = ALLOC_P(heap, Column)(column_name, TypeId(type), len, col_ind, nullable, unique);
  } else {
    column = ALLOC_P(heap, Column)(column_name, TypeId(type), col_ind, nullable, unique);
  }
  return offset;
}
