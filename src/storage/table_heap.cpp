#include "storage/table_heap.h"
bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
  if(row.GetSerializedSize(schema_) > TablePage::SIZE_MAX_ROW){ return false; }
  uint32_t free = free_space_map.begin()->first;
  page_id_t page_id = free_space_map.begin()->second;
  auto table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
  pop_heap(free_space_map.begin(), free_space_map.end());
  free_space_map.pop_back();
  if(free < row.GetSerializedSize(schema_) + PAGE_SIZE_TUPLE){
    buffer_pool_manager_->UnpinPage(page_id, false);
    page_id_t next_page_id;
    table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(next_page_id));
    if (last_page_id != INVALID_PAGE_ID) {
      auto prev_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(last_page_id));
      prev_table_page->SetNextPageId(next_page_id);
      buffer_pool_manager_->UnpinPage(last_page_id, true);
    }
    table_page->Init(next_page_id, last_page_id, log_manager_, txn);
    table_page->InsertTuple(row, schema_, txn,lock_manager_, log_manager_);
    buffer_pool_manager_->UnpinPage(next_page_id, true);
    free_space_map.emplace_back(pair(table_page->GetFreeSpaceRemaining(),table_page->GetTablePageId()));
    push_heap(free_space_map.begin(), free_space_map.end());
    last_page_id = next_page_id;
    //分配新页
  }else{
    auto slot_vector = reusable_slot_map[page_id];
    if(!slot_vector.empty()){
      row.SetRowId(RowId(page_id,*slot_vector.rbegin()));
      slot_vector.pop_back();
    }
    table_page->InsertTuple(row, schema_, txn,lock_manager_, log_manager_);
    buffer_pool_manager_->UnpinPage(table_page->GetPageId(), true);
    free_space_map.emplace_back( pair(table_page->GetFreeSpaceRemaining(),table_page->GetTablePageId()));
    push_heap(free_space_map.begin(), free_space_map.end());
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

void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {page_id_t page_id = rid.GetPageId();
  if(buffer_pool_manager_->IsPageFree(page_id)) return;
  MarkDelete(rid, txn);
  if(row_will_delete_map.find(page_id)==row_will_delete_map.end()){
    row_will_delete_map.insert(pair(page_id, rid.GetSlotNum()));
  }
  row_will_delete_map[page_id].emplace_back(rid.GetSlotNum());
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

void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if(temp_table_page->GetNextPageId() != INVALID_PAGE_ID) DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  }else{
    DeleteTable(first_page_id_);
  }
}


bool TableHeap::GetTuple(Row *row, Transaction *txn) {
  page_id_t page_id = row->GetRowId().GetPageId();
  if(buffer_pool_manager_->IsPageFree(page_id)) return false;
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
  buffer_pool_manager_->UnpinPage(page_id, false);
  return page->GetTuple(row, schema_, txn, lock_manager_);
}

TableIterator TableHeap::Begin(Transaction *txn) {
  return TableIterator(this, txn);
}

TableIterator TableHeap::End() {
  return TableIterator();
}
