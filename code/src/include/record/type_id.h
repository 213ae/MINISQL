#ifndef MINISQL_TYPE_ID_H
#define MINISQL_TYPE_ID_H

enum TypeId : uint8_t {
  kTypeInvalid = 0,
  kTypeInt,
  kTypeFloat,
  kTypeChar,
  KMaxTypeId = kTypeChar
};

#endif //MINISQL_TYPE_ID_H
