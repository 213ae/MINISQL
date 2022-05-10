#include "page/bitmap_page.h"
#include "glog/logging.h"

template<size_t PageSize>
uint32_t BitmapPage<PageSize>::FindNextFreePage() {
  for (uint32_t i = 0; i < MAX_CHARS; ++i) {
    auto temp = (uint8_t)bytes[i];
    if(temp < 0xff){
      for (int j = 0; j < 8; ++j) {
        if((temp & (0x80 >> j)) == 0) return i * 8 + j;
      }
      LOG(FATAL) << "something wrong in FindNextFreePage().1" << endl;
    }
  }
  LOG(FATAL) << "something wrong in FindNextFreePage().2" << endl;
  return 0;//该return不会发生
}

template<size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  if(page_allocated_ < GetMaxSupportedSize()) {//检查该位图页管理的页中是否有空闲页
    if(next_free_page_ < GetMaxSupportedSize()){//在所有的页都被分配过一次之前，先按页的顺序从小到大分配
      page_offset = next_free_page_;
      next_free_page_++;
    }else{//在所有的页都至少被分配过一次之后，仍有可用的页，说明有页被删除，查找被删除的页
      page_offset = FindNextFreePage();
    }
    //更新位图页元信息
    uint32_t byte_index = page_offset / 8;
    uint32_t bit_index = page_offset % 8;
    if(IsPageFree(page_offset) == true){
      bytes[byte_index] |= (0x80 >> bit_index);
    }else{
      LOG(FATAL) << "something wrong in AllocatePage().2" << endl;
    }
    page_allocated_++;
    return true;
  }else{
    LOG(INFO) << "no available page" << endl;
    return false;
  }
}

template<size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  if(IsPageFree(page_offset) == false) {
    //更新位图页元信息
    uint32_t byte_index = page_offset / 8;
    uint32_t bit_index = page_offset % 8;
    bytes[byte_index] &= (0xff ^ (0x80 >> bit_index));
    page_allocated_--;
    return true;
  }else{
    LOG(WARNING) << "this page is free" << endl;
    return false;
  }
}

template<size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  if(page_offset >= 0 && page_offset < GetMaxSupportedSize()){
    uint32_t byte_index = page_offset / 8;
    uint32_t bit_index = page_offset % 8;
    return IsPageFreeLow(byte_index, bit_index);
  }
  else {
    LOG(INFO) << "wrong page_offset:" << page_offset << endl;
    return false;
  }
}

template<size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  auto temp = (uint8_t)bytes[byte_index];
  uint8_t flag = (temp >> (7 - bit_index)) % 2;
  return !flag;
}

template
class BitmapPage<64>;

template
class BitmapPage<128>;

template
class BitmapPage<256>;

template
class BitmapPage<512>;

template
class BitmapPage<1024>;

template
class BitmapPage<2048>;

template
class BitmapPage<4096>;