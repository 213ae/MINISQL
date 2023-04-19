#ifndef MINISQL_INSTANCE_H
#define MINISQL_INSTANCE_H

#include <memory>
#include <string>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include "buffer/buffer_pool_manager.h"
#include "catalog/catalog.h"
#include "common/config.h"
#include "common/dberr.h"
#include "storage/disk_manager.h"

class DBStorageEngine {
public:
  explicit DBStorageEngine(std::string db_name, bool init = true,
                           uint32_t buffer_pool_size = DEFAULT_BUFFER_POOL_SIZE)
          : db_file_name_( db_name), init_(init) {
    // Init database file if needed
    DIR *dir;
    if((dir = opendir("./databases")) == nullptr)  mkdir("./databases",0777);
    if (init_) {
      remove(("./databases/" + db_file_name_).c_str());
    }
    // Initialize components
    string backup_path = "./databases/." + db_name;
    fstream backup(backup_path, ios::binary | ios::in | ios::out);
    fstream dbfile("./databases/" + db_file_name_);
    if (dbfile.is_open()) {
      if (backup.is_open()) {
        ofstream origin("./databases/" + db_file_name_, ios::binary | ios::trunc | ios::out);
        origin << backup.rdbuf();
        origin.close();
        backup.close();
      } else {
        backup.clear();
        backup.open(backup_path, ios::binary | ios::trunc | ios::out);
        ifstream origin("./databases/" + db_name, ios::binary | ios::in);
        backup << origin.rdbuf();
        origin.close();
        backup.close();
      }
      dbfile.close();
    }
    disk_mgr_ = new DiskManager("./databases/" + db_file_name_);
    bpm_ = new BufferPoolManager(buffer_pool_size, disk_mgr_);
    // Allocate static page for db storage engine
    if (init) {
      page_id_t id;
      if(!(bpm_->NewPage(id) != nullptr && id == CATALOG_META_PAGE_ID)) LOG(FATAL) << "Failed to allocate catalog meta page.";
      if(!(bpm_->NewPage(id) != nullptr && id == INDEX_ROOTS_PAGE_ID)) LOG(FATAL) << "Failed to allocate header page.";
      bpm_->UnpinPage(CATALOG_META_PAGE_ID, false);
      bpm_->UnpinPage(INDEX_ROOTS_PAGE_ID, false);
    } else {
      ASSERT(!bpm_->IsPageFree(CATALOG_META_PAGE_ID), "Invalid catalog meta page.");
      ASSERT(!bpm_->IsPageFree(INDEX_ROOTS_PAGE_ID), "Invalid header page.");
    }
    catalog_mgr_ = new CatalogManager(bpm_, nullptr, nullptr, init);
  }

  ~DBStorageEngine() {
    delete catalog_mgr_;
    delete bpm_;
    delete disk_mgr_;
    remove(("./databases/." + db_file_name_).c_str());
  }

public:
  DiskManager *disk_mgr_;
  BufferPoolManager *bpm_;
  CatalogManager *catalog_mgr_;
  std::string db_file_name_;
  bool init_;
};

#endif //MINISQL_INSTANCE_H
