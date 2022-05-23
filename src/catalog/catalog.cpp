#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
  uint32_t offset = 0;
  MACH_WRITE_UINT32(buf + offset, table_meta_pages_.size());
  offset += sizeof(uint32_t);
  for (auto itr : table_meta_pages_) {
    MACH_WRITE_TO(table_id_t, buf + offset, itr.first);
    offset += sizeof(table_id_t);
    MACH_WRITE_TO(page_id_t, buf + offset, itr.second);
    offset += sizeof(page_id_t);
  }
  MACH_WRITE_UINT32(buf + offset, index_meta_pages_.size());
  offset += sizeof(uint32_t);
  for (auto itr : index_meta_pages_) {
    MACH_WRITE_TO(table_id_t, buf + offset, itr.first);
    offset += sizeof(table_id_t);
    MACH_WRITE_TO(page_id_t, buf + offset, itr.second);
    offset += sizeof(page_id_t);
  }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf, MemHeap *heap) {
  auto catalog_meta = NewInstance(heap);
  uint32_t offset = 0, table_num, index_num;
  table_id_t temp_table_id;
  index_id_t temp_index_id;
  page_id_t temp_page_id;
  table_num = MACH_READ_UINT32(buf + offset);
  offset += sizeof(uint32_t);
  for (uint32_t i = 0; i < table_num; ++i) {
    temp_table_id = MACH_READ_FROM(table_id_t, buf + offset);
    offset += sizeof(table_id_t);
    temp_page_id = MACH_READ_FROM(page_id_t, buf + offset);
    offset += sizeof(page_id_t);
    catalog_meta->table_meta_pages_.emplace(temp_table_id, temp_page_id);
  }
  index_num = MACH_READ_UINT32(buf + offset);
  offset += sizeof(uint32_t);
  for (uint32_t i = 0; i < index_num; ++i) {
    temp_index_id = MACH_READ_FROM(index_id_t , buf + offset);
    offset += sizeof(index_id_t);
    temp_page_id = MACH_READ_FROM(page_id_t, buf + offset);
    offset += sizeof(page_id_t);
    catalog_meta->index_meta_pages_.emplace(temp_index_id, temp_page_id);
  }
  return catalog_meta;
}

uint32_t CatalogMeta::GetSerializedSize() const {
  return table_meta_pages_.size() * (sizeof(table_id_t) + sizeof(page_id_t)) +
         index_meta_pages_.size() * (sizeof(index_id_t) + sizeof(page_id_t)) +
         2 * sizeof(uint32_t);
}

CatalogMeta::CatalogMeta() {}


CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
        : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager),
          log_manager_(log_manager), heap_(new SimpleMemHeap()) {
  if(init){
    catalog_meta_ = CatalogMeta::NewInstance(heap_);
    next_table_id_ = 0;
    next_index_id_ = 0;
  }else{
    Page* catalog_meta_page =  buffer_pool_manager->FetchPage(CATALOG_META_PAGE_ID);
    catalog_meta_ = CatalogMeta::DeserializeFrom(catalog_meta_page->GetData(), heap_);
    for(auto itr : catalog_meta_->table_meta_pages_){
      if(itr.second != INVALID_PAGE_ID) LoadTable(itr.first, itr.second);
    }
    for(auto itr : catalog_meta_->index_meta_pages_){
      if(itr.second != INVALID_PAGE_ID) LoadIndex(itr.first, itr.second);
    }
    next_table_id_ = catalog_meta_->GetNextTableId();
    next_index_id_ = catalog_meta_->GetNextIndexId();
    buffer_pool_manager->UnpinPage(CATALOG_META_PAGE_ID, false);
  }
}

CatalogManager::~CatalogManager() {
  delete heap_;
}

dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema,
                                    Transaction *txn, TableInfo *&table_info) {
  page_id_t new_table_page_id;
  table_id_t new_table_id = next_table_id_;
  if(table_names_.find(table_name) != table_names_.end()) return DB_TABLE_ALREADY_EXIST;
  auto new_page = buffer_pool_manager_->NewPage(new_table_page_id);
  if(new_page == nullptr) return DB_FAILED;
  //更新catalog_meta
  catalog_meta_->table_meta_pages_[new_table_id] = new_table_page_id;
  catalog_meta_->table_meta_pages_[new_table_id + 1] = INVALID_PAGE_ID;
  auto catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  catalog_meta_->SerializeTo(catalog_meta_page->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  //返回new_table_info
  table_info = TableInfo::Create(heap_);
  auto table_heap = TableHeap::Create(buffer_pool_manager_, schema, txn, log_manager_, lock_manager_, table_info->GetMemHeap());
  auto table_metadata =  TableMetadata::Create(new_table_id, table_name, table_heap->GetFirstPageId(), schema, table_info->GetMemHeap());
  table_metadata->SerializeTo(new_page->GetData());
  table_info->Init(table_metadata, table_heap);
  buffer_pool_manager_->UnpinPage(new_table_page_id, true);
  //更新CatalogManager
  next_table_id_++;
  table_names_.emplace(table_name, new_table_id);
  tables_.emplace(new_table_id, table_info);
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  if(table_names_.find(table_name) == table_names_.end()) return DB_TABLE_NOT_EXIST;
  return GetTable(table_names_[table_name], table_info);
}

dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  for(auto itr : tables_){
    tables.emplace_back(itr.second);
  }
  return DB_SUCCESS;
}

dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info) {
  if(table_names_.find(table_name) == table_names_.end()) return DB_TABLE_NOT_EXIST;
  page_id_t new_index_page_id;
  index_id_t new_index_id = next_index_id_;
  if(index_names_[table_name].find(index_name) != index_names_[table_name].end()) return DB_INDEX_ALREADY_EXIST;
  auto new_page = buffer_pool_manager_->NewPage(new_index_page_id);
  if(new_page == nullptr) return DB_FAILED;
  //更新catalog_meta
  catalog_meta_->index_meta_pages_[new_index_id] = new_index_page_id;
  catalog_meta_->index_meta_pages_[new_index_id + 1] = INVALID_PAGE_ID;
  auto catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  catalog_meta_->SerializeTo(catalog_meta_page->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  //返回new_index_info
  index_info = IndexInfo::Create(heap_);
  vector<uint32_t> key_map;
  for(const auto& column_name : index_keys){
    bool is_exist = false;
    for(auto column : tables_[table_names_[table_name]]->GetSchema()->GetColumns()){
      if(column->GetName() == column_name) { is_exist = true; break; }
    }
    if(!is_exist) return DB_COLUMN_NAME_NOT_EXIST;
  }
  for(auto column : tables_[table_names_[table_name]]->GetSchema()->GetColumns()){
    if(std::find(index_keys.begin(), index_keys.end(), column->GetName()) != index_keys.end()) key_map.emplace_back(column->GetTableInd());
  }
  auto index_metadata =  IndexMetadata::Create(new_index_id, index_name, table_names_[table_name], key_map, index_info->GetMemHeap());
  index_metadata->SerializeTo(new_page->GetData());
  index_info->Init(index_metadata, tables_[table_names_[table_name]], buffer_pool_manager_);
  buffer_pool_manager_->UnpinPage(new_index_page_id, true);
  //更新CatalogManager
  next_index_id_++;
  index_names_[table_name].emplace(index_name, new_index_id);
  indexes_.emplace(new_index_id, index_info);
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  if(index_names_.at(table_name).find(index_name) == index_names_.at(table_name).end()) return DB_INDEX_NOT_FOUND;
  index_info = indexes_.at(index_names_.at(table_name).at(index_name));
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  for(const auto& index : index_names_.at(table_name)){
    indexes.emplace_back(indexes_.at(index.second));
  }
  return DB_SUCCESS;
}

dberr_t CatalogManager::DropTable(const string &table_name) {
  if(table_names_.find(table_name) == table_names_.end()) return DB_TABLE_NOT_EXIST;
  table_id_t table_id = table_names_[table_name];
  tables_[table_id]->GetTableHeap()->FreeHeap();
  buffer_pool_manager_->DeletePage(catalog_meta_->table_meta_pages_[table_id]);
  catalog_meta_->table_meta_pages_.erase(table_id);
  auto catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  catalog_meta_->SerializeTo(catalog_meta_page->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  heap_->Free(tables_[table_id]);
  tables_.erase(table_id);
  table_names_.erase(table_name);
  return DB_SUCCESS;
}

dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  if(index_names_[table_name].find(index_name) == index_names_[table_name].end()) return DB_INDEX_NOT_FOUND;
  index_id_t index_id = index_names_[table_name][index_name];
  indexes_[index_id]->GetIndex()->Destroy();
  buffer_pool_manager_->DeletePage(catalog_meta_->index_meta_pages_[index_id]);
  catalog_meta_->index_meta_pages_.erase(index_id);
  auto catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  catalog_meta_->SerializeTo(catalog_meta_page->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  heap_->Free(indexes_[index_id]);
  indexes_.erase(index_id);
  index_names_[table_name].erase(index_name);
  return DB_SUCCESS;
}


dberr_t CatalogManager::FlushCatalogMetaPage() const {
  if(buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID)) return DB_SUCCESS;
  else return DB_FAILED;
}

dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  Page *temp_page = buffer_pool_manager_->FetchPage(page_id);
  if(temp_page == nullptr) return DB_FAILED;
  TableInfo *table_info = TableInfo::Create(heap_);
  TableMetadata* table_mata = nullptr;
  TableMetadata::DeserializeFrom(temp_page->GetData(), table_mata, table_info->GetMemHeap());
  if(table_mata == nullptr) return DB_FAILED;
  TableHeap* table_heap = TableHeap::Create(buffer_pool_manager_, table_mata->GetFirstPageId(), table_mata->GetSchema(), log_manager_, lock_manager_, table_info->GetMemHeap());
  table_info->Init(table_mata, table_heap);
  table_names_.emplace(table_mata->GetTableName(), table_id);
  tables_.emplace(table_id, table_info);
  buffer_pool_manager_->UnpinPage(page_id, false);
  return DB_SUCCESS;
}

dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  Page *temp_page = buffer_pool_manager_->FetchPage(page_id);
  if(temp_page == nullptr) return DB_FAILED;
  IndexInfo *index_info = IndexInfo::Create(heap_);
  IndexMetadata* index_mata = nullptr;
  IndexMetadata::DeserializeFrom(temp_page->GetData(), index_mata, index_info->GetMemHeap());
  if(index_mata == nullptr) return DB_FAILED;
  index_info->Init(index_mata, tables_[index_mata->GetTableId()], buffer_pool_manager_);
  index_names_[tables_[index_mata->GetTableId()]->GetTableName()].emplace(index_mata->GetIndexName(), index_id);
  indexes_.emplace(index_id, index_info);
  buffer_pool_manager_->UnpinPage(page_id, false);
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  if(tables_.find(table_id) == tables_.end()) return DB_FAILED;
  table_info = tables_[table_id];
  return DB_SUCCESS;
}