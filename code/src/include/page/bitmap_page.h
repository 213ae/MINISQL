#ifndef MINISQL_BITMAP_PAGE_H
#define MINISQL_BITMAP_PAGE_H

#include <bitset>
#include <vector>
#include <algorithm>
#include "common/macros.h"
#include "common/config.h"
using namespace std;
template<size_t PageSize>
class BitmapPage {
 public:
  void Init();
  /**
   * @return The number of pages that the bitmap page can record, i.e. the capacity of an extent.
   */
  static constexpr size_t GetMaxSupportedSize() { return 8 * MAX_CHARS; }

  /**
   * @param page_offset （是返回值，通过它获取到分配的页号）Index in extent of the page allocated.
   * @return true if successfully allocate a page.
   */
  bool AllocatePage(uint32_t &page_offset);

  /**
   * @return true if successfully de-allocate a page.
   */
  bool DeAllocatePage(uint32_t page_offset);

  /**
   * @return whether a page in the extent is free
   */
  [[nodiscard]] bool IsPageFree(uint32_t page_offset) const;

  vector<page_id_t> *GetDeletedList() {return deleted_list;}

 private:
  /**
   * check a bit(byte_index, bit_index) in bytes is free(value 0).
   *
   * @param byte_index value of page_offset / 8
   * @param bit_index value of page_offset % 8
   * @return true if a bit is 0, false if 1.
   */
  [[nodiscard]] bool IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const;

  /** Note: need to update if modify page structure.
   * MAX_CHARS也就是BITMAP_CONTENT_SIZE的计算公式，更改元信息（比如增加一个堆的指针）时该公式需更新
   */
  static constexpr size_t MAX_CHARS = PageSize - 2 * sizeof(uint32_t) - sizeof(vector<page_id_t> *);


 private:
  /** The space occupied by all members of the class should be equal to the PageSize */
  [[maybe_unused]] uint32_t page_allocated_{};
  [[maybe_unused]] uint32_t next_free_page_{};
  [[maybe_unused]] vector<page_id_t> *deleted_list{nullptr};
  [[maybe_unused]] unsigned char bytes[MAX_CHARS]{};
};

#endif //MINISQL_BITMAP_PAGE_H
