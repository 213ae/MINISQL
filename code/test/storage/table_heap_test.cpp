#include <vector>
#include <unordered_map>
#include "common/instance.h"
#include "gtest/gtest.h"
#include "record/field.h"
#include "record/schema.h"
#include "storage/table_heap.h"
#include "utils/utils.h"

static string db_file_name = "table_heap_test.db";
using Fields = std::vector<Field>;
using namespace std;
void PrintRow(Row row){
  cout << row.GetRowId().GetPageId() << " " <<  row.GetRowId().GetSlotNum() << " " << row.GetFieldCount() << " ";
  for(auto field : row.GetFields()){
    field->PrintField();
  }
  cout << endl;
}
TEST(TableHeapTest, TableHeapSampleTest) {
  // init testing instance
  DBStorageEngine engine(db_file_name);
  SimpleMemHeap heap;
  const int row_nums = 1000000;
  // create schema
  std::vector<Column *> columns = {ALLOC_COLUMN(heap)("id", TypeId::kTypeInt, 0, false, false),
                                   ALLOC_COLUMN(heap)("name", TypeId::kTypeChar, 64, 1, true, false),
                                   ALLOC_COLUMN(heap)("account", TypeId::kTypeFloat, 2, true, false)};
  auto schema = std::make_shared<Schema>(columns);
  // create rows
  std::unordered_map<int64_t, Fields *> row_values;
  TableHeap *table_heap = TableHeap::Create(engine.bpm_, schema.get(), nullptr, nullptr, nullptr, &heap);
  for (int i = 0; i < row_nums; i++) {
    int32_t len = RandomUtils::RandomInt(0, 64);
    char *characters = new char[len];
    RandomUtils::RandomString(characters, len);
    Fields *fields =
        new Fields{Field(TypeId::kTypeInt, i), Field(TypeId::kTypeChar, const_cast<char *>(characters), len, true),
                   Field(TypeId::kTypeFloat, RandomUtils::RandomFloat(-999.f, 999.f))};
    Row row(*fields);
    table_heap->InsertTuple(row, nullptr);
    if(row_values.find(row.GetRowId().Get()) != row_values.end()) LOG(INFO) << row.GetRowId().GetPageId() << row.GetRowId().GetSlotNum();
    row_values[row.GetRowId().Get()] = fields;
    delete[] characters;
    LOG_EVERY_N(INFO, 10000) << i;
  }

/*  for (int i = 0; i < 5; i++) {
    RowId rid(2, i * 2);
    table_heap->ApplyDelete(rid, nullptr);
  }*/
  engine.bpm_->CheckAllUnpinned();
  /*for (TableIterator itr = table_heap->Begin(); itr != table_heap->End(); ++itr) {
    // PrintRow(*itr);
  }*/
  /*engine.bpm_->CheckAllUnpinned();
  delete table_heap;
  engine.bpm_->CheckAllUnpinned();*/
  //TableHeap *table_heap01 = TableHeap::Create(engine.bpm_, 2, schema.get(), nullptr, nullptr, &heap);
  /*for (TableIterator itr = table_heap01->Begin(); itr != table_heap->End(); ++itr) {
    // PrintRow(*itr);
  }*/
  engine.bpm_->CheckAllUnpinned();
  ASSERT_EQ(row_nums, row_values.size());
  int i = 0;
  for (auto row_kv : row_values) {
    i++;
    Row row(RowId(row_kv.first));
    if(!table_heap->GetTuple(&row, nullptr)) {
      LOG(INFO) << "something wrong";
    }
    ASSERT_EQ(schema.get()->GetColumnCount(), row.GetFields().size());
    for (size_t j = 0; j < schema.get()->GetColumnCount(); j++) {
      ASSERT_EQ(CmpBool::kTrue, row.GetField(j)->CompareEquals(row_kv.second->at(j)));
    }
    // free spaces
    delete row_kv.second;
  }
}

