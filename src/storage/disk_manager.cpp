#include <stdexcept>
#include <sys/stat.h>

#include "glog/logging.h"
#include "page/bitmap_page.h"
#include "storage/disk_manager.h"

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close() {
  WritePhysicalPage(META_PAGE_ID, meta_data_);
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  if(!IsPageFree(logical_page_id)) ReadPhysicalPage(MapPageId(logical_page_id), page_data);
  else LOG(WARNING) << "Invalid page id.";
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  if (!IsPageFree(logical_page_id)) WritePhysicalPage(MapPageId(logical_page_id), page_data);
  else LOG(WARNING) << "Invalid page id.";

}

page_id_t DiskManager::AllocatePage() {
  auto *meta_page = reinterpret_cast<DiskFileMetaPage *>(meta_data_);//获取磁盘元数据
  uint32_t page_id = -1;
  char bitmap_buf[PAGE_SIZE];
  if(meta_page->num_allocated_pages_ >= MAX_VALID_PAGE) LOG(FATAL) << "Maximum available pages reached";//检查分配页数是否达到上限
  if(meta_page->num_extents_ == 0){//第一次使用，需初始化磁盘管理器元数据，开辟新的分区
    //初始化位图
    memset(bitmap_buf, 0, PAGE_SIZE);
    auto *bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmap_buf);
    //获取页号，更新位图
    bitmap->AllocatePage(page_id);
    WritePhysicalPage(static_cast<page_id_t>(meta_page->num_extents_ * (BITMAP_SIZE + 1) + 1), bitmap_buf);
    //更新元数据
    meta_page->num_extents_++;
    meta_page->num_allocated_pages_++;
    meta_page->extent_used_page_[meta_page->num_extents_ - 1]++;
  }else{
    uint32_t i = 0;
    for (; i < meta_page->num_extents_; ++i) {//遍历分区
      if(meta_page->extent_used_page_[i] < BITMAP_SIZE){//该分区未满
        //获取位图
        ReadPhysicalPage(static_cast<page_id_t>(i * (BITMAP_SIZE + 1) + 1), bitmap_buf);
        auto *bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmap_buf);
        //获取页号，更新位图
        bitmap->AllocatePage(page_id);
        WritePhysicalPage(static_cast<page_id_t>(i * (BITMAP_SIZE + 1) + 1), bitmap_buf);
        //逻辑页号 = 分配页在分区i中的页号 + 分区i之前所有分区页数总和
        page_id += i * BITMAP_SIZE;
        //更新元数据
        meta_page->num_allocated_pages_++;
        meta_page->extent_used_page_[i]++;
        break;
      }
    }
    if(i == meta_page->num_extents_){//所有分区都已满，则开辟新的分区
      //初始化位图
      memset(bitmap_buf, 0, PAGE_SIZE);
      auto *bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmap_buf);
      //获取页号，更新位图
      bitmap->AllocatePage(page_id);
      WritePhysicalPage(static_cast<page_id_t>(meta_page->num_extents_ * (BITMAP_SIZE + 1) + 1), bitmap_buf);
      //逻辑页号 = 分配页在分区i中的页号 + 分区i之前所有分区页数总和
      page_id += meta_page->num_extents_ * BITMAP_SIZE;
      //更新元数据
      meta_page->num_extents_++;
      meta_page->num_allocated_pages_++;
      meta_page->extent_used_page_[meta_page->num_extents_ - 1]++;
    }
  }
  return static_cast<page_id_t>(page_id);
}

void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  auto *meta_page = reinterpret_cast<DiskFileMetaPage *>(meta_data_);
  // 初始化位图
  char bitmap_buf[PAGE_SIZE];
  page_id_t no_bitmap = logical_page_id / (page_id_t)BITMAP_SIZE;  // 分区号
  uint32_t page_offset = logical_page_id % BITMAP_SIZE;            // 页号
  // 根据分区号读取对应的位图页
  ReadPhysicalPage(static_cast<page_id_t>(no_bitmap * (BITMAP_SIZE + 1) + 1), bitmap_buf);
  auto *bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmap_buf);
  // 成功删除则更新位图，更新元数据
  if (bitmap->DeAllocatePage(page_offset)) {
    WritePhysicalPage(static_cast<page_id_t>(no_bitmap * (BITMAP_SIZE + 1) + 1), bitmap_buf);
    meta_page->num_allocated_pages_--;
    meta_page->extent_used_page_[no_bitmap]--;
  }
}

bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  char bitmap_buf[PAGE_SIZE];
  page_id_t no_bitmap = logical_page_id / (page_id_t)BITMAP_SIZE;//分区号
  uint32_t page_offset = logical_page_id % BITMAP_SIZE;//页号
  //根据分区号读取对应的位图页
  ReadPhysicalPage(static_cast<page_id_t>(no_bitmap * (BITMAP_SIZE + 1) + 1), bitmap_buf);
  auto *bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmap_buf);
  return bitmap->IsPageFree(page_offset);
}

page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  return 2 + logical_page_id / (BITMAP_SIZE / 8) + logical_page_id;
}

int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {//读取未被分配的页的情况
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);//设置要读的页在文件中的偏移量
    db_io_.read(page_data, PAGE_SIZE);//从该偏移量开始，读一页
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {//读取内容不足一页的情况
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}