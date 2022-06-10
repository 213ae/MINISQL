#ifndef MINISQL_EXECUTE_ENGINE_H
#define MINISQL_EXECUTE_ENGINE_H

#include <string>
#include <unordered_map>
#include <sstream>
#include <dirent.h>
#include <iomanip>
#include <stdexcept>
#include "common/dberr.h"
#include "common/instance.h"
#include "transaction/transaction.h"
#include <ctime>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/time.h>

extern "C" {
int yyparse(void);
#include "parser/minisql_lex.h"
#include "parser/parser.h"
}

class RowidCompare{
 public:
  bool operator()(RowId rid1, RowId rid2)
  {return rid1.Get() < rid2.Get();}
};

/**
 * ExecuteContext stores all the context necessary to run in the execute engine
 * This struct is implemented by student self for necessary.
 *
 * eg: transaction info, execute result...
 */
struct ExecuteContext {
  char* DBname{nullptr};
  char* table_name{nullptr};
  char* index_name{nullptr};
  string index_type{"bptree"};
  //clock_t start;//计时器
  struct timeval start;
  struct timeval end{};
  int col_id{0};//创建新表时待分配col_id
  uint rows_num{0};//计数器
  bool print_flag{0};//PrintInfo两种不同的输出格式
  bool flag_quit_{false};
  bool dont_print{false};//执行文件时，中途不输出
  bool has_condition{false};
  bool is_and{false};//是否为and，是则在最后一个result_index_container的基础上更新，否或者result_index_container为空创建新container
  bool index_only{true};
  bool has_end{false};
  vector<uint32_t> key_map;//主键列号，添加索引的列号，select的列号
  vector<uint32_t> unique_col;//unique列的列号
  vector<Column *> columns;//表的模式
  vector<string> condition_colname;
  vector<vector<RowId>> result_rids_container;
  vector<vector<uint32_t>> result_index_container;//存放临时结果
  unordered_map<string, IndexInfo*> index_on_one_key;
  unordered_map<int64_t, uint32_t> rowid2index_map;//索引查找时用来将RowId转换成对应行的容器下标
  unordered_map<uint32_t, int64_t> index2rowid_map;
  unordered_map<string, vector<string>> value_i_of_col_;//某一列的所有值
  unordered_map<uint, Field> update_field_map;
  Transaction *txn_{nullptr};
};

/**
 * ExecuteEngine
 */
class ExecuteEngine {
public:
  ExecuteEngine();

  ~ExecuteEngine() {
    for (const auto& it : dbs_) {
      delete it.second;
    }
  }

  /**
   * executor interface
   */
  dberr_t Execute(pSyntaxNode ast, ExecuteContext *context);

private:
  void ExecuteNodeColumnDefinition(pSyntaxNode ast, ExecuteContext *context);

  void ExecuteNodeColumnList(pSyntaxNode ast, ExecuteContext *context);

  void ExecuteNodeConnector(pSyntaxNode ast, ExecuteContext *context);

  void ExecuteNodeUpdateValue(pSyntaxNode ast, ExecuteContext *context);

  void ExecuteNodeCompareOperator(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteSelect(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteInsert(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteDelete(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteUpdate(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteQuit(pSyntaxNode ast, ExecuteContext *context);

  void Next(pSyntaxNode ast, ExecuteContext *context);

  void FindConditionColumn(pSyntaxNode ast, ExecuteContext *context);

private:
  [[maybe_unused]] std::unordered_map<std::string, DBStorageEngine *> dbs_;  /** all opened databases */
  [[maybe_unused]] std::string current_db_;  /** current database */
};

#endif //MINISQL_EXECUTE_ENGINE_H
