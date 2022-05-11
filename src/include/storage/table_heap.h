#ifndef MINISQL_TABLE_HEAP_H
#define MINISQL_TABLE_HEAP_H

#include "buffer/buffer_pool_manager.h"
#include "page/table_page.h"
#include "storage/table_iterator.h"
#include "transaction/log_manager.h"
#include "transaction/lock_manager.h"

class TableHeap {
  friend class TableIterator;

public:
  static TableHeap *Create(BufferPoolManager *buffer_pool_manager, Schema *schema, Transaction *txn,
                           LogManager *log_manager, LockManager *lock_manager, MemHeap *heap) {
    void *buf = heap->Allocate(sizeof(TableHeap));
    return new(buf) TableHeap(buffer_pool_manager, schema, txn, log_manager, lock_manager);
  }

  static TableHeap *Create(BufferPoolManager *buffer_pool_manager, page_id_t first_page_id, Schema *schema,
                           LogManager *log_manager, LockManager *lock_manager, MemHeap *heap) {
    void *buf = heap->Allocate(sizeof(TableHeap));
    return new(buf) TableHeap(buffer_pool_manager, first_page_id, schema, log_manager, lock_manager);
  }

  ~TableHeap() {
    FreeHeap();
  }


  /*todo 每次插入会遍历一遍页再遍历一遍槽直到找到可复用槽，没有找到再创建新槽，
   *    可以在table_heap类中维护一个free_space_map<page_id_t, uint32_t>用于查找合适的页，
   *    和reusable_slot_map<page_id_t,vector<uint32_t>>，用于查找某一页可复用槽
   *    在table_heap有first_page_id构造时遍历一遍所有页和槽对free_space_map和reusable_slot_map进行初始化，
   *    每一页free_space赋值给free_space_map[page_id]将页中每个可复用槽号push进map[page_id]，
   *    插入时先找free_space能容纳待插入元组的页，然后从map[page_id]中pop一个可复用槽号，vector为空则创建新槽
   *    为了实现这一改动，TablePage::GetFreeSpaceRemaining()需变为public，
   *    TablePage::InsertTuple()传入的参数row需明确槽号（通过修改row.rid.slot_num）以便直接定位，
   *    若槽号为0，则直接开辟新槽
   */
  /*todo 每次更新和实际删除都会把页中所有的槽遍历一遍并更新位置，有较大优化空间
   *  可将实际删除都换成标记删除，并把要实际删除的元组的rid push进row_will_delete_map<page_id_t, vector<uint32_t>>，
   *  在析构函数中遍历map进行实际删除#（简称标析删），要更新的元组采取先标析删再插入，这种设计也便于回滚
   *  缺点：可能会改变表中相近的数据在磁盘中的的聚集性，增加查询成本
   *  #此处的删除也要优化，采用一次删除一页的待删除槽的做法
   */

  /**
   * Insert a tuple into the table. If the tuple is too large (>= page_size), return false.
   * @param[in/out] row Tuple Row to insert, the rid of the inserted tuple is wrapped in object row
   * @param[in] txn The transaction performing the insert
   * @return true iff the insert is successful
   */
  bool InsertTuple(Row &row, Transaction *txn);

  /**
   * Mark the tuple as deleted. The actual delete will occur when ApplyDelete is called.
   * @param[in] rid Resource id of the tuple of delete
   * @param[in] txn Transaction performing the delete
   * @return true iff the delete is successful (i.e the tuple exists)
   */
  bool MarkDelete(const RowId &rid, Transaction *txn);

  /**
   * if the new tuple is too large to fit in the old page, return false (will delete and insert)
   * @param[in] row Tuple of new row
   * @param[in] rid Rid of the old tuple
   * @param[in] txn Transaction performing the update
   * @return true is update is successful.
   */
  bool UpdateTuple(Row &row, const RowId &rid, Transaction *txn);

  /**
   * Called on Commit/Abort to actually delete a tuple or rollback an insert.
   * @param rid Rid of the tuple to delete
   * @param txn Transaction performing the delete.
   */
  void ApplyDelete(const RowId &rid, Transaction *txn);

  /**
   * Called on abort to rollback a delete.
   * @param[in] rid Rid of the deleted tuple.
   * @param[in] txn Transaction performing the rollback
   */
  void RollbackDelete(const RowId &rid, Transaction *txn);

  /**
   * Read a tuple from the table.
   * @param[in/out] row Output variable for the tuple, row id of the tuple is wrapped in row
   * @param[in] txn transaction performing the read
   * @return true if the read was successful (i.e. the tuple exists)
   */
  bool GetTuple(Row *row, Transaction *txn);

  /**
   * Free table heap and release storage in disk file
   */
  void FreeHeap();

  /**
   * @return the begin iterator of this table
   */
  TableIterator Begin(Transaction *txn = nullptr);

  /**
   * @return the end iterator of this table
   */
  TableIterator End();

  /**
   * @return the id of the first page of this table
   */
  inline page_id_t GetFirstPageId() const { return first_page_id_; }

private:
  /**
   * create table heap and initialize first page
   */
  explicit TableHeap(BufferPoolManager *buffer_pool_manager, Schema *schema, Transaction *txn,
                     LogManager *log_manager, LockManager *lock_manager)
        : buffer_pool_manager_(buffer_pool_manager),
          schema_(schema),
          log_manager_(log_manager),
          lock_manager_(lock_manager) {
    auto *table_page = reinterpret_cast<TablePage *>(buffer_pool_manager->NewPage(first_page_id_));
    table_page->Init(first_page_id_, INVALID_PAGE_ID, log_manager, txn);
    buffer_pool_manager->UnpinPage(first_page_id_, true);
    //从bufmgr获取新磁盘页，并对页初始化
  }

  /**
   * load existing table heap by first_page_id
   */
  explicit TableHeap(BufferPoolManager *buffer_pool_manager, page_id_t first_page_id, Schema *schema,
                     LogManager *log_manager, LockManager *lock_manager)
          : buffer_pool_manager_(buffer_pool_manager),
            first_page_id_(first_page_id),
            schema_(schema),
            log_manager_(log_manager),
            lock_manager_(lock_manager) {
  }

private:
  BufferPoolManager *buffer_pool_manager_;
  page_id_t first_page_id_;
  Schema *schema_;
  [[maybe_unused]] LogManager *log_manager_;
  [[maybe_unused]] LockManager *lock_manager_;
};

#endif  // MINISQL_TABLE_HEAP_H
