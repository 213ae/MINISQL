#include "storage/table_heap.h"
//todo 空间不足时分配新页
bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
  if(row.GetSerializedSize(schema_) > TablePage::SIZE_MAX_ROW){ return false; }
  auto *table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  bool flag;
  while((flag = !table_page->InsertTuple(row, schema_, txn,lock_manager_, log_manager_)) && table_page->GetNextPageId() != INVALID_PAGE_ID){
    buffer_pool_manager_->UnpinPage(table_page->GetPageId(), false);
    table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(table_page->GetNextPageId()));
  }
  if(!flag){
    page_id_t next_page_id, cur_page_id = table_page->GetPageId();
    buffer_pool_manager_->NewPage(next_page_id);
    table_page->SetNextPageId(next_page_id);
    buffer_pool_manager_->UnpinPage(cur_page_id, true);
    table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(next_page_id));
    table_page->Init(next_page_id, cur_page_id, log_manager_, txn);
    table_page->InsertTuple(row, schema_, txn,lock_manager_, log_manager_);
    buffer_pool_manager_->UnpinPage(next_page_id, true);
    //分配新页
  }
  return true;
}

bool TableHeap::MarkDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Transaction *txn) {
  //检验rid
  //1、检验page_id，是否free
  //2.检验slot_num，是否为空，是否被删除或被标记为删除(GetTuple自动检查）
  page_id_t page_id = rid.GetPageId();
  Row old_row(rid);
  if(buffer_pool_manager_->IsPageFree(page_id)) return false;
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
  if(!page->GetTuple(&old_row, schema_, txn, lock_manager_)) {
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    return false;
  }
  int flag = page->UpdateTuple(row, &old_row, schema_, txn, lock_manager_, log_manager_);
  if(flag == 0){
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
    return true;
  }else if(flag == 1){
    //先删除后插入
    ApplyDelete(rid, txn);
    InsertTuple(row, txn);
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
    return true;
  }else{
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    return false;
  }
}

void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
  // Step1: Find the page which contains the tuple.
  // Step2: Delete the tuple from the page.
  page_id_t page_id = rid.GetPageId();
  if(buffer_pool_manager_->IsPageFree(page_id)) return;
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
  page->ApplyDelete(rid, txn, log_manager_);
  buffer_pool_manager_->UnpinPage(page_id, true);
}

void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback the delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::FreeHeap() {
  //貌似不能做些啥
  //把buffer中的数据页flush到磁盘
}

bool TableHeap::GetTuple(Row *row, Transaction *txn) {
  page_id_t page_id = row->GetRowId().GetPageId();
  if(buffer_pool_manager_->IsPageFree(page_id)) return false;
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
  return page->GetTuple(row, schema_, txn, lock_manager_);
}

TableIterator TableHeap::Begin(Transaction *txn) {
  return TableIterator(this, txn);
}

TableIterator TableHeap::End() {
  return TableIterator();
}
