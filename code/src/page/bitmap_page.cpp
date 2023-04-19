#include "page/bitmap_page.h"
#include "glog/logging.h"
template<size_t PageSize>
void BitmapPage<PageSize>::Init(){
  deleted_list = new vector<page_id_t>;
  if(page_allocated_ < next_free_page_){
    for(uint32_t i = 0; i < next_free_page_; i++){
      if(IsPageFree(i)){
        deleted_list->emplace_back(i);
      }
      if(deleted_list->size() == next_free_page_ - page_allocated_) break;
    }
  }
}


template<size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  if(page_allocated_ < GetMaxSupportedSize()) {//检查该位图页管理的页中是否有空闲页
    if(page_allocated_ < next_free_page_){
      page_offset = deleted_list->back();
      deleted_list->pop_back();
    }else{
      page_offset = next_free_page_;
      next_free_page_++;
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
    deleted_list->emplace_back(page_offset);
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