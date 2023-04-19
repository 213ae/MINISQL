#ifndef MINISQL_B_PLUS_TREE_INTERNAL_PAGE_H
#define MINISQL_B_PLUS_TREE_INTERNAL_PAGE_H

#include <queue>
#include <string.h>
#include "page/b_plus_tree_page.h"

#define INTERNAL_PAGE_HEADER_SIZE 24
#define INTERNAL_PAGE_SIZE ((PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (sizeof(MappingType)) - 1)
/**
 * Store n indexed keys and n+1 child pointers (page_id) within internal page.
 * Pointer PAGE_ID(i) points to a subtree in which all keys K satisfy:
 * K(i) <= K < K(i+1).
 * NOTE: since the number of keys does not equal to number of child pointers,
 * the first key always remains invalid. That is to say, any search/lookup
 * should ignore the first key.
 *
 * Internal page format (keys are stored in increasing order):
 *  --------------------------------------------------------------------------
 * | HEADER | KEY(1)+PAGE_ID(1) | KEY(2)+PAGE_ID(2) | ... | KEY(n)+PAGE_ID(n) |
 *  --------------------------------------------------------------------------
 */
class BPlusTreeInternalPage : public BPlusTreePage {
public:
  // must call initialize method after "create" a new node
  void Init(page_id_t page_id, page_id_t parent_id = INVALID_PAGE_ID, int max_size = INTERNAL_PAGE_SIZE);

  GenericKey KeyAt(int index) const;

  void SetKeyAt(int index, const GenericKey *key);

  int ValueIndex(const RowId &value) const;

  RowId ValueAt(int index) const;

  RowId Lookup(const GenericKey *key, const KeyProcessor &comparator) const;

  void PopulateNewRoot(const RowId &old_value, const GenericKey *new_key, const RowId &new_value);

  int InsertNodeAfter(const RowId &old_value, const GenericKey *new_key, const RowId &new_value);

  void Remove(int index);

  RowId RemoveAndReturnOnlyChild();

  // Split and Merge utility methods
  void MoveAllTo(BPlusTreeInternalPage *recipient, const GenericKey *middle_key, BufferPoolManager *buffer_pool_manager);

  void MoveHalfTo(BPlusTreeInternalPage *recipient, BufferPoolManager *buffer_pool_manager);

  void MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const GenericKey *middle_key,
                        BufferPoolManager *buffer_pool_manager);

  void MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const GenericKey *middle_key,
                         BufferPoolManager *buffer_pool_manager);

private:
  void CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager);

  void CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager);

  void CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager);

  MappingType array_[0];
};

#endif  // MINISQL_B_PLUS_TREE_INTERNAL_PAGE_H
