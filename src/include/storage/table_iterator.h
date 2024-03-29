#ifndef MINISQL_TABLE_ITERATOR_H
#define MINISQL_TABLE_ITERATOR_H

#include "common/rowid.h"
#include "record/row.h"
#include "transaction/transaction.h"


class TableHeap;
/**
 * 只读前向迭代器，可用于遍历表
 */
class TableIterator {

public:
  // you may define your own constructor based on your member variables
  explicit TableIterator();

  explicit TableIterator(TableHeap* table_heap, Transaction* txn = nullptr);

  TableIterator(const TableIterator &other);

  virtual ~TableIterator();

  inline int64_t GetRowId_64() { return rid.Get(); }

  inline Row* GetRow() { return row; }

  inline bool operator==(const TableIterator &itr) const { return rid == itr.rid; }

  inline bool operator!=(const TableIterator &itr) const { return !(*this == itr); }

  const Row &operator*();

  Row *operator->();

  TableIterator &operator++();

  TableIterator operator++(int);

private:
  TableHeap* table_heap{nullptr};
  RowId rid;
  Row *row{nullptr};
  Transaction* txn;

  // add your own private member variables here
};

#endif //MINISQL_TABLE_ITERATOR_H
