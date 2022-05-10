#include "common/macros.h"
#include "storage/table_iterator.h"
#include "storage/table_heap.h"

TableIterator::TableIterator() = default;

TableIterator::TableIterator(TableHeap* table_heap, Transaction* txn) : table_heap(table_heap), txn(txn){
  auto *page = reinterpret_cast<TablePage *>(table_heap->buffer_pool_manager_->FetchPage(table_heap->GetFirstPageId()));
  page->GetFirstTupleRid(&rid);
  row = new Row(rid);
  table_heap->GetTuple(row, txn);
}

TableIterator::TableIterator(const TableIterator &other) {
  table_heap = other.table_heap;
  rid = other.rid;
  txn = other.txn;
  new (row)Row(*other.row);
}

TableIterator::~TableIterator() = default;

bool TableIterator::operator==(const TableIterator &itr) const {
  return rid == itr.rid;
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return !(*this == itr);
}

const Row &TableIterator::operator*() {
  if(rid.GetPageId() != INVALID_PAGE_ID) {
    return *row;
  }else{
    LOG(ERROR) << "Invalid row";
    row->SetRowId(rid);
    return *row;
  }
}

Row *TableIterator::operator->() {
  if(rid.GetPageId() != INVALID_PAGE_ID) {
    return row;
  }else{
    return nullptr;
  }
}

TableIterator &TableIterator::operator++() {
  auto *page = reinterpret_cast<TablePage *>(table_heap->buffer_pool_manager_->FetchPage(rid.GetPageId()));
  RowId next_rid;
  if(page->GetNextTupleRid(rid, &next_rid)) {
    rid = next_rid;
    row->SetRowId(rid);
    table_heap->GetTuple(row, txn);
  }else if(page->GetNextPageId() != INVALID_PAGE_ID){
    page = reinterpret_cast<TablePage *>(table_heap->buffer_pool_manager_->FetchPage(page->GetNextPageId()));
    page->GetFirstTupleRid(&rid);
    row->SetRowId(rid);
    table_heap->GetTuple(row, txn);
  }else{
    new (this)TableIterator();
  }
  return *this;
}

TableIterator TableIterator::operator++(int) {
  TableIterator old(*this);
  ++*this;
  return old;
}
