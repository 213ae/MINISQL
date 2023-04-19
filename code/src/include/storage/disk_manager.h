#ifndef DISK_MGR_H
#define DISK_MGR_H

#include <atomic>
#include <fstream>
#include <iostream>
#include <mutex>
#include <string>
#include <vector>
#include "common/config.h"
#include "common/macros.h"
#include "page/bitmap_page.h"
#include "page/disk_file_meta_page.h"

/**
 * DiskManager takes care of the allocation and de allocation of pages within a database. It performs the reading and
 * writing of pages to and from disk, providing a logical file layer within the context of a database management system.
 *
 * Disk page storage format: (Free Page BitMap Size = PAGE_SIZE * 8, we note it as N)
 * | Meta Page | Free Page BitMap 1 | Page 1 | Page 2 | ....
 *      | Page N | Free Page BitMap 2 | Page N+1 | ... | Page 2N | ... |
 */
class DiskManager {
 public:
  /**
  * 读取存放数据库所有信息的文件，用该文件保存的元信息初始化
  * @param db_file 存放数据库信息的文件名
   */
  explicit DiskManager(const std::string &db_file);

  ~DiskManager() {
    if (!closed) {
      Close();
    }
    for(auto bitmap_ptr : bitmaps_){
      if(bitmap_ptr.second == nullptr) {
        std::cout << "Bitmap_ptr is nullptr.";
      }else {
        delete bitmap_ptr.second->GetDeletedList();
        delete bitmap_ptr.second;
      }
    }
  }

  /**
   * Read page from specific page_id
   * Note: page_id = 0 is reserved for disk meta page
   * 从磁盘读取逻辑页号为logical_page_id的数据页到page_data[]中
   */
  void ReadPage(page_id_t logical_page_id, char *page_data);

  /**
   * Write data to specific page
   * Note: page_id = 0 is reserved for disk meta page
   * 将page_data中的数据写到逻辑页号为logical_page_id的数据页中
   */
  void WritePage(page_id_t logical_page_id, const char *page_data);

  /**
   * Get next free page from disk
   * @return logical page id of allocated page
   * 根据位图页信息，从磁盘获取可用数据页页号，设置该页状态为已分配
   */
  page_id_t AllocatePage();

  /**
   * Free this page and reset bit map
   * 释放逻辑页号为logical_page_id的数据页
   */
  void DeAllocatePage(page_id_t logical_page_id);

  /**
   * Return whether specific logical_page_id is free
   */
  bool IsPageFree(page_id_t logical_page_id);

  /**
   * Shut down the disk manager and close all the file resources.
   * 将当前元信息写回数据库文件，并关闭数据库文件
   */
  void Close();

  /**
   * Get Meta Page
   * Note: Used only for debug
   */
  char *GetMetaData() {
    return meta_data_;
  }

  static constexpr size_t BITMAP_SIZE = BitmapPage<PAGE_SIZE>::GetMaxSupportedSize();

 private:
  /**
   * Helper function to get disk file size
   */
  int GetFileSize(const std::string &file_name);

  /**
   * Read physical page from disk
   */
  void ReadPhysicalPage(page_id_t physical_page_id, char *page_data);

  /**
   * Write data to physical page in disk
   */
  void WritePhysicalPage(page_id_t physical_page_id, const char *page_data);

  /**
   * Map logical page id to physical page id
   */
  page_id_t MapPageId(page_id_t logical_page_id);

 private:
  // stream to write db file
  std::fstream db_io_;
  std::string file_name_;
  // with multiple buffer pool instances, need to protect file access
  std::recursive_mutex db_io_latch_;
  unordered_map<int, BitmapPage<PAGE_SIZE>*> bitmaps_;//todo bitmap的读和写可以放到DiskManager的构造和析构函数中统一进行
  bool closed{false};
  //保存了分配的总页数，分区数，每个分区使用的页数
  char meta_data_[PAGE_SIZE];
};

#endif