#include "buffer/buffer_pool_manager.h"
#include "glog/logging.h"
#include "page/bitmap_page.h"

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
        : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page: page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  if(page_id == INVALID_PAGE_ID) {
    LOG(WARNING) << "Can not fetch invalid page id";
    return nullptr;
  }
  if(disk_manager_->IsPageFree(page_id)){
    LOG(WARNING) << "This page is not allocated.";
    return nullptr;
  }
  //在缓冲区，从缓冲区取
  if(page_table_.find(page_id) != page_table_.end()) {
    // 1.1    If P exists, pin it and return it immediately.
    replacer_->Pin(page_table_[page_id]);
    pages_[page_table_[page_id]].pin_count_++;
    return pages_ + page_table_[page_id];
  }
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  //不在缓冲区，从磁盘读取
  frame_id_t frame_id_will_used;
  if(!free_list_.empty()){//优先从free_list取页
    frame_id_will_used = free_list_.front();
    free_list_.pop_front();
    //从磁盘读取页，初始化页对象，更新page_table，固定页，更新页
    disk_manager_->ReadPage(page_id, pages_[frame_id_will_used].data_);
    pages_[frame_id_will_used].page_id_ = page_id;
    page_table_.insert(pair(page_id, frame_id_will_used));
    replacer_->Pin(frame_id_will_used);
    pages_[frame_id_will_used].pin_count_++;
    return &pages_[frame_id_will_used];
  }else{//free_list为空，从replacer_取
    if(replacer_->Victim(&frame_id_will_used)){
      // 2.     If R is dirty, write it back to the disk.
      if(pages_[frame_id_will_used].IsDirty()){
        disk_manager_->WritePage(page_table_[frame_id_will_used],pages_[frame_id_will_used].data_);
        pages_[frame_id_will_used].is_dirty_ = false;
      }
      // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
      disk_manager_->ReadPage(page_id, pages_[frame_id_will_used].data_);
      pages_[frame_id_will_used].page_id_ = page_id;
      // 3.     Delete R from the page table and insert P.
      page_table_.erase(frame_id_will_used);
      page_table_.insert(pair(page_id, frame_id_will_used));
      replacer_->Pin(frame_id_will_used);
      pages_[frame_id_will_used].pin_count_++;
      return &pages_[frame_id_will_used];
    }else{
      LOG(WARNING) << "There are no available frame_id.";
    }
  }
  return nullptr;
}

Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  //LOG(INFO) << "new page";
  frame_id_t frame_id_will_used;
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  if(!free_list_.empty()){
    frame_id_will_used = free_list_.front();
    free_list_.pop_front();
    // 3.   Update P's metadata, zero out memory and add P to the page table.
    page_id = AllocatePage();
    pages_[frame_id_will_used].page_id_ = page_id;
    page_table_.insert(pair(page_id, frame_id_will_used));
    // 4.   Set the page ID output parameter. Return a pointer to P.
    replacer_->Pin(frame_id_will_used);
    pages_[frame_id_will_used].pin_count_++;
    return &pages_[frame_id_will_used];
  }else{
    if(replacer_->Victim(&frame_id_will_used)){
      if(pages_[frame_id_will_used].IsDirty()){
        disk_manager_->WritePage(page_table_[frame_id_will_used],pages_[frame_id_will_used].data_);
        pages_[frame_id_will_used].is_dirty_ = false;
      }
      // 3.   Update P's metadata, zero out memory and add P to the page table.
      page_id = AllocatePage();
      pages_[frame_id_will_used].ResetMemory();
      pages_[frame_id_will_used].page_id_ = page_id;
      page_table_.erase(frame_id_will_used);
      page_table_.insert(pair(page_id, frame_id_will_used));
      // 4.   Set the page ID output parameter. Return a pointer to P.
      replacer_->Pin(frame_id_will_used);
      pages_[frame_id_will_used].pin_count_++;
      return &pages_[frame_id_will_used];
    }else{
      LOG(WARNING) << "There are no available frame_id.";
    }
  }
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  return nullptr;
}

bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  if(page_table_.find(page_id) != page_table_.end()) {
    // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
    if(pages_[page_table_[page_id]].pin_count_ > 0) {
      return false;
    }else{
      // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
      DeallocatePage(page_id);
      pages_[page_table_[page_id]].page_id_ = INVALID_PAGE_ID;
      pages_[page_table_[page_id]].is_dirty_ = false;
      pages_[page_table_[page_id]].pin_count_ = 0;
      pages_[page_table_[page_id]].ResetMemory();
      free_list_.insert(std::lower_bound(free_list_.begin(), free_list_.end(), page_table_[page_id]), page_table_[page_id]);
      page_table_.erase(page_id);
    }
  }
  // 1.   If P does not exist, return true.
  return true;
}

bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  if(page_id == INVALID_PAGE_ID) {
    LOG(INFO) << "Can not unpin INVALID_PAGE_ID";
    return false;
  }
  if(page_table_.find(page_id) != page_table_.end()) {
    if(pages_[page_table_[page_id]].pin_count_ > 0) {
      if(!pages_[page_table_[page_id]].is_dirty_) pages_[page_table_[page_id]].is_dirty_ = is_dirty;
      if((--pages_[page_table_[page_id]].pin_count_) == 0) replacer_->Unpin(page_table_[page_id]);
      return true;
    }
    else{
      LOG(INFO) << "It is unpined";
      return false ;
    }
  }else{
    LOG(WARNING) << "This page don't exist in page_table_";
    return false;
  }
}

bool BufferPoolManager::FlushPage(page_id_t page_id) {
  if(page_table_.find(page_id) != page_table_.end()) {
    disk_manager_->WritePage(page_id, pages_[page_table_[page_id]].data_);
    pages_[page_table_[page_id]].is_dirty_ = false;
    return true;
  }else{
    return false;
  }
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}