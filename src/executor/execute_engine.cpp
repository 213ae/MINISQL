#include "executor/execute_engine.h"
#include "glog/logging.h"
#include "parser/minisql_lex.h"

inline void PrintTable(const vector<string>& header, const vector<vector<string>>& i_col_j_row, const vector<uint>& output_index = {}){
  uint col_num = header.size();
  uint row_num;
  vector<uint> max_width;
  FILE *fp = fopen("result.txt", "w");
  if(output_index.empty()){//全部输出
    row_num = i_col_j_row[0].size();
    for (uint i = 0; i < col_num; ++i) {
      max_width.emplace_back(header[i].length());
      for(uint j = 0; j < row_num; j++) { if(i_col_j_row[i][j].length() > max_width[i]) max_width[i] = i_col_j_row[i][j].length(); }
    }
    for(uint i = 0; i < col_num; ++i) { fputc('+', fp);for(uint j = 0; j < max_width[i] + 2; j++) fputc('-', fp); } fprintf(fp, "+\n");
    for(uint i = 0; i < col_num; ++i) { fprintf(fp, "| %-*s ", max_width[i], header[i].c_str()); } fprintf(fp, "|\n");
    for(uint i = 0; i < col_num; ++i) { fputc('+', fp);for(uint j = 0; j < max_width[i] + 2; j++) fputc('-', fp); }  fprintf(fp, "+\n");
    for(uint j = 0; j < row_num; j++){
      for(uint i = 0; i < col_num; i++){ fprintf(fp, "| %-*s ", max_width[i], i_col_j_row[i][j].c_str()); } fprintf(fp, "|\n");
    }
    for(uint i = 0; i < col_num; ++i) { fputc('+', fp);for(uint j = 0; j < max_width[i] + 2; j++) fputc('-', fp); }  fprintf(fp, "+\n");
    fclose(fp);
    uint start = 0, end = 30;
    max_width.clear();
    do{
      if(end > row_num) end = row_num;
      for (uint i = 0; i < col_num; ++i) {
        max_width.emplace_back(header[i].length());
        for(uint j = start; j < end; j++) { if(i_col_j_row[i][j].length() > max_width[i]) max_width[i] = i_col_j_row[i][j].length(); }
      }
      for(uint i = 0; i < col_num; ++i) { putchar('+');for(uint j = 0; j < max_width[i] + 2; j++) putchar('-'); } printf("+\n");
      for(uint i = 0; i < col_num; ++i) { printf("| %-*s ", max_width[i], header[i].c_str()); } printf("|\n");
      for(uint i = 0; i < col_num; ++i) { putchar('+');for(uint j = 0; j < max_width[i] + 2; j++) putchar('-'); } printf("+\n");
      for(uint j = start; j < end; j++){
        for(uint i = 0; i < col_num; i++){ printf("| %-*s ", max_width[i], i_col_j_row[i][j].c_str()); } printf("|\n");
      }
      for(uint i = 0; i < col_num; ++i) { putchar('+');for(uint j = 0; j < max_width[i] + 2; j++) putchar('-'); } printf("+\n");
      if(end == row_num) break;
      start += 30; end += 30;
      cout << "(Press enter to continue or press 'q' to quit)\n:";
      if(getchar() == 'q') {getchar(); break; }
    }while(true);
    cout << "Done. The complete result is in 'result.txt'" << endl;
  }else{
    row_num = output_index.size();
    for (uint i = 0; i < col_num; ++i) {
      max_width.emplace_back(header[i].length());
      for(uint j : output_index) { if(i_col_j_row[i][j].length() > max_width[i]) max_width[i] = i_col_j_row[i][j].length(); }
    }
    for(uint i = 0; i < col_num; ++i) { fputc('+', fp);for(uint j = 0; j < max_width[i] + 2; j++) fputc('-', fp); } fprintf(fp, "+\n");
    for(uint i = 0; i < col_num; ++i) { fprintf(fp, "| %-*s ", max_width[i], header[i].c_str()); } fprintf(fp, "|\n");
    for(uint i = 0; i < col_num; ++i) { fputc('+', fp);for(uint j = 0; j < max_width[i] + 2; j++) fputc('-', fp); } fprintf(fp, "+\n");
    for(uint j : output_index){
      for(uint i = 0; i < col_num; i++){ fprintf(fp, "| %-*s ", max_width[i], i_col_j_row[i][j].c_str()); } fprintf(fp, "|\n");
    }
    for(uint i = 0; i < col_num; ++i) { fputc('+', fp);for(uint j = 0; j < max_width[i] + 2; j++) fputc('-', fp); } fprintf(fp, "+\n");
    fclose(fp);
    uint start = 0, end = 30;
    max_width.clear();
    do{
      if(end >= output_index.size()) end = output_index.size();
      for (uint i = 0; i < col_num; ++i) {
        max_width.emplace_back(header[i].length());
        for(uint j = start; j < end; j++) { if(i_col_j_row[i][j].length() > max_width[i]) max_width[i] = i_col_j_row[i][j].length(); }
      }
      for(uint i = 0; i < col_num; ++i) { putchar('+');for(uint j = 0; j < max_width[i] + 2; j++) putchar('-'); } printf("+\n");
      for(uint i = 0; i < col_num; ++i) { printf("| %-*s ", max_width[i], header[i].c_str()); } printf("|\n");
      for(uint i = 0; i < col_num; ++i) { putchar('+');for(uint j = 0; j < max_width[i] + 2; j++) putchar('-'); } printf("+\n");
      for(uint j = start; j < end; j++){
        for(uint i = 0; i < col_num; i++){ printf("| %-*s ", max_width[i], i_col_j_row[i][output_index[j]].c_str()); } printf("|\n");
      }
      for(uint i = 0; i < col_num; ++i) { putchar('+');for(uint j = 0; j < max_width[i] + 2; j++) putchar('-'); } printf("+\n");
      if(end == output_index.size()) break;
      start += 30; end += 30;
      cout << "(Press enter to continue or press 'q' to quit)\n:";
      if(getchar() == 'q') {getchar(); break; }
    }while(true);
    cout << "Done. The complete result is in 'result.txt'" << endl;
  }
}
inline void PrintInfo(ExecuteContext *context){
  struct timeval end{};
  gettimeofday(&end, nullptr);
  if(!context->dont_print) {
    if (context->print_flag == 0)
      if (end.tv_usec < context->start.tv_usec)
        cout << "Query OK, " << context->rows_num << " row affected (" << end.tv_sec - context->start.tv_sec - 1 << "."
             << setw(6) << setfill('0') << 1000000 + end.tv_usec - context->start.tv_usec << " sec)" << endl;
      else
        cout << "Query OK, " << context->rows_num << " row affected (" << end.tv_sec - context->start.tv_sec << "."
             << setw(6) << setfill('0') << end.tv_usec - context->start.tv_usec << " sec)" << endl;
    else {
      if (context->rows_num != 0) {
        if (end.tv_usec < context->start.tv_usec)
          cout << context->rows_num << " rows in set (" << end.tv_sec - context->start.tv_sec - 1 << "." << setw(6)
               << setfill('0') << 1000000 + end.tv_usec - context->start.tv_usec << " sec)" << endl;
        else
          cout << context->rows_num << " rows in set (" << end.tv_sec - context->start.tv_sec << "." << setw(6)
               << setfill('0') << end.tv_usec - context->start.tv_usec << " sec)" << endl;
      } else {
        if (end.tv_usec < context->start.tv_usec)
          cout << "Empty set (" << end.tv_sec - context->start.tv_sec - 1 << "." << setw(6) << setfill('0')
               << 1000000 + end.tv_usec - context->start.tv_usec << " sec)" << endl;
        else
          cout << "Empty set (" << end.tv_sec - context->start.tv_sec << "." << setw(6) << setfill('0')
               << end.tv_usec - context->start.tv_usec << " sec)" << endl;
      }
    }
  }
}

ExecuteEngine::ExecuteEngine() {
  char path[] = "./databases";
  DIR *dir;
  if((dir = opendir(path)) == nullptr)  mkdir("./databases",0777);
  struct dirent *stdir;
  while((stdir = readdir(dir)) != nullptr)
  {
    if( strcmp( stdir->d_name , "." ) == 0 ||
        strcmp( stdir->d_name , "..") == 0 ||
        stdir->d_name[0] == '.')
      continue;
    dbs_[stdir->d_name] = new DBStorageEngine(stdir->d_name, false);
  }
  closedir(dir);
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast, ExecuteContext *context) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context);
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context);
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context);
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context);
    case kNodeShowTables:
      return ExecuteShowTables(ast, context);
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context);
    case kNodeDropTable:
      return ExecuteDropTable(ast, context);
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context);
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context);
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context);
    case kNodeSelect:
      return ExecuteSelect(ast, context);
    case kNodeInsert:
      return ExecuteInsert(ast, context);
    case kNodeDelete:
      return ExecuteDelete(ast, context);
    case kNodeUpdate:
      return ExecuteUpdate(ast, context);
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context);
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context);
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context);
    case kNodeExecFile:
      return ExecuteExecfile(ast, context);
    case kNodeQuit:
      return ExecuteQuit(ast, context);
    default:
      break;
  }
  return DB_FAILED;
}

void ExecuteEngine::FindConditionColumn(pSyntaxNode ast, ExecuteContext *context){
  if(ast->type_ == kNodeConditions) context->has_condition = true;
  else{
    if(context->has_condition){
      if(ast->type_ == kNodeIdentifier){
        context->condition_colname.emplace_back(ast->val_);
      }
    }
  }
  if(ast->child_ != nullptr) FindConditionColumn(ast->child_, context);
  if(ast->next_ != nullptr) FindConditionColumn(ast->next_, context);
}

void ExecuteEngine::Next(pSyntaxNode ast, ExecuteContext *context) {
  switch (ast->type_) {
    case kNodeColumnDefinition: {
      bool is_unique = false;
      if (ast->val_ != nullptr && strcmp("unique", ast->val_) == 0) {
        is_unique = true;
        context->unique_col.emplace_back(context->col_id);
      }
      string col_name = ast->child_->val_;
      string type = ast->child_->next_->val_;
      if (type == "int") {
        auto *new_column = new Column(col_name, kTypeInt, context->col_id++, false, is_unique);
        context->columns.emplace_back(new_column);
      }
      if (type == "float") {
        auto *new_column = new Column(col_name, kTypeFloat, context->col_id++, false, is_unique);
        context->columns.emplace_back(new_column);
      }
      if (type == "char") {
        string len_str = ast->child_->next_->child_->val_;
        int len = stoi(len_str);
        if (len > 4000 || len < 0 || len_str.find('-') != string::npos || len_str.find('.') != string::npos)
          throw invalid_argument("Invalid char length");
        if (len == 0 && is_unique) throw logic_error("The used storage engine can't index column '" + col_name + "'");
        auto *new_column = new Column(col_name, kTypeChar, len, context->col_id++, true, is_unique);
        context->columns.emplace_back(new_column);
      }
      if (ast->next_ != nullptr) Next(ast->next_, context);
      break;
    }
    case kNodeColumnList: {
      string type = ast->val_;
      pSyntaxNode temp_ast = ast->child_;
      while (temp_ast != nullptr) {
        bool is_exist = false;
        for (auto col : context->columns) {
          if (col->GetName() == temp_ast->val_) {
            context->key_map.emplace_back(col->GetTableInd());
            is_exist = true;
            break;
          }
        }
        if (!is_exist) {
          if (type == "primary keys" || type == "index keys") {
            string error_info = "Key column '";
            error_info += temp_ast->val_;
            error_info += "' doesn't exist in table";
            throw invalid_argument(error_info);
          } else if (type == "select columns") {
            string error_info = " Unknown column '";
            error_info += temp_ast->val_;
            error_info += "' in 'field list'";
            throw invalid_argument(error_info);
          }
        }
        temp_ast = temp_ast->next_;
      }
      if (ast->next_ != nullptr) Next(ast->next_, context);
      break;
    }
    case kNodeConnector: {
      string connector = ast->val_;
      if (connector == "and") {
        if (ast->child_->type_ != kNodeConnector &&
            context->index_on_one_key.find(ast->child_->child_->val_) != context->index_on_one_key.end()) {
          Next(ast->child_->next_, context);
          context->is_and = true;
          Next(ast->child_, context);
        } else {
          Next(ast->child_, context);
          context->is_and = true;
          Next(ast->child_->next_, context);
        }
      } else if (connector == "or") {
        Next(ast->child_, context);
        context->is_and = false;
        Next(ast->child_->next_, context);
      }
      break;
    }
    case kNodeCompareOperator: {
      // context->has_condition = true;
      string compare_operator = ast->val_;  //<, >, =, <>, <=, >=, is, not
      string col_name = ast->child_->val_;
      bool is_exist = false;
      for (auto col : context->columns) {
        if (col->GetName() == col_name) {
          is_exist = true;
          break;
        }
      }
      if (!is_exist) {
        string error_info = "Unknown column '";
        error_info += col_name;
        error_info += "' in 'where clause'";
        throw invalid_argument(error_info);
      }
      vector<uint32_t> result;
      if (compare_operator == "is" || compare_operator == "not") {
        if (compare_operator == "is") {
          if (context->is_and) {
            for (auto i : context->result_index_container.back()) {
              if (context->value_i_of_col_[col_name][i] == "null") result.emplace_back(i);
            }
            context->result_index_container.pop_back();
            context->result_index_container.emplace_back(result);
          } else {
            for (uint i = 0; i < context->value_i_of_col_[col_name].size(); i++) {
              if (context->value_i_of_col_[col_name][i] == "null") result.emplace_back(i);
            }
            context->result_index_container.emplace_back(result);
          }
        } else {
          if (context->is_and) {
            for (auto i : context->result_index_container.back()) {
              if (context->value_i_of_col_[col_name][i] != "null") result.emplace_back(i);
            }
            context->result_index_container.pop_back();
            context->result_index_container.emplace_back(result);
          } else {
            for (uint i = 0; i < context->value_i_of_col_[col_name].size(); i++) {
              if (context->value_i_of_col_[col_name][i] != "null") result.emplace_back(i);
            }
            context->result_index_container.emplace_back(result);
          }
        }
      } else if (compare_operator == "<" || compare_operator == ">" || compare_operator == "<=" ||
                 compare_operator == ">=" || compare_operator == "<>" || compare_operator == "=") {
        TypeId type = kTypeInvalid;
        string value = ast->child_->next_->val_;
        for (auto col : context->columns) {
          if (col->GetName() == col_name) {
            type = col->GetType();
          }
        }
        if (compare_operator == "<") {
          if (context->index_on_one_key.find(col_name) != context->index_on_one_key.end()) {
            vector<Field> fields;
            switch (type) {
              case kTypeInt:
                fields.emplace_back(Field(type, atoi(value.c_str())));
                break;
              case kTypeFloat:
                fields.emplace_back(Field(type, (float)atof(value.c_str())));
                break;
              case kTypeChar: {
                char str[value.length() + 1];
                strcpy(str, value.c_str());
                fields.emplace_back(Field(type, str, value.length(), false));
                break;
              }
              default:
                break;
            }

            vector<RowId> result_rid;
            context->index_on_one_key[col_name]->GetIndex()->ScanKey(Row(fields), result_rid, context->txn_, "<");
            if (!context->index_only) {
              for (auto rid : result_rid) {
                result.emplace_back(context->rowid2index_map[rid.Get()]);
              }
              sort(result.begin(), result.end());
              if (context->is_and) {
                vector<uint> temp_vector;
                set_intersection(context->result_index_container.back().begin(),
                                 context->result_index_container.back().end(), result.begin(), result.end(),
                                 back_inserter(temp_vector));
                context->result_index_container.pop_back();
                context->result_index_container.emplace_back(temp_vector);
              } else {
                context->result_index_container.emplace_back(result);
              }
            } else {
              sort(result_rid.begin(), result_rid.end(), RowidCompare());
              if (context->is_and) {
                vector<RowId> temp_vector;
                set_intersection(context->result_rids_container.back().begin(),
                                 context->result_rids_container.back().end(), result_rid.begin(), result_rid.end(),
                                 back_inserter(temp_vector), RowidCompare());
                context->result_rids_container.pop_back();
                context->result_rids_container.emplace_back(temp_vector);
              } else {
                context->result_rids_container.emplace_back(result_rid);
              }
            }

          } else {
            if (context->is_and) {
              switch (type) {
                case kTypeInt: {
                  for (auto i : context->result_index_container.back()) {
                    if (context->value_i_of_col_[col_name][i] == "null") continue;
                    if (atoi(context->value_i_of_col_[col_name][i].c_str()) < atoi(value.c_str())) {
                      result.emplace_back(i);
                    }
                  }
                  break;
                }
                case kTypeFloat: {
                  for (auto i : context->result_index_container.back()) {
                    if (context->value_i_of_col_[col_name][i] == "null") continue;
                    if (atof(context->value_i_of_col_[col_name][i].c_str()) < atof(value.c_str())) {
                      result.emplace_back(i);
                    }
                  }
                  break;
                }
                case kTypeChar: {
                  for (auto i : context->result_index_container.back()) {
                    if (context->value_i_of_col_[col_name][i] == "null") continue;
                    if (context->value_i_of_col_[col_name][i] < value) {
                      result.emplace_back(i);
                    }
                  }
                  break;
                }
                default:
                  break;
              }
              context->result_index_container.pop_back();
              context->result_index_container.emplace_back(result);
            } else {
              switch (type) {
                case kTypeInt: {
                  for (uint i = 0; i < context->value_i_of_col_[col_name].size(); i++) {
                    if (context->value_i_of_col_[col_name][i] == "null") continue;
                    if (atoi(context->value_i_of_col_[col_name][i].c_str()) < atoi(value.c_str())) {
                      result.emplace_back(i);
                    }
                  }
                  break;
                }
                case kTypeFloat: {
                  for (uint i = 0; i < context->value_i_of_col_[col_name].size(); i++) {
                    if (context->value_i_of_col_[col_name][i] == "null") continue;
                    if (atof(context->value_i_of_col_[col_name][i].c_str()) < atof(value.c_str())) {
                      result.emplace_back(i);
                    }
                  }
                  break;
                }
                case kTypeChar: {
                  for (uint i = 0; i < context->value_i_of_col_[col_name].size(); i++) {
                    if (context->value_i_of_col_[col_name][i] == "null") continue;
                    if (context->value_i_of_col_[col_name][i] < value) {
                      result.emplace_back(i);
                    }
                  }
                  break;
                }
                default:
                  break;
              }
              context->result_index_container.emplace_back(result);
            }
          }
        } else if (compare_operator == "<=") {
          if (context->index_on_one_key.find(col_name) != context->index_on_one_key.end()) {
            vector<Field> fields;
            switch (type) {
              case kTypeInt:
                fields.emplace_back(Field(type, atoi(value.c_str())));
                break;
              case kTypeFloat:
                fields.emplace_back(Field(type, (float)atof(value.c_str())));
                break;
              case kTypeChar: {
                char str[value.length() + 1];
                strcpy(str, value.c_str());
                fields.emplace_back(Field(type, str, value.length(), false));
                break;
              }
              default:
                break;
            }

            vector<RowId> result_rid;
            context->index_on_one_key[col_name]->GetIndex()->ScanKey(Row(fields), result_rid, context->txn_, "<=");
            if (!context->index_only) {
              for (auto rid : result_rid) {
                result.emplace_back(context->rowid2index_map[rid.Get()]);
              }
              sort(result.begin(), result.end());
              if (context->is_and) {
                vector<uint> temp_vector;
                set_intersection(context->result_index_container.back().begin(),
                                 context->result_index_container.back().end(), result.begin(), result.end(),
                                 back_inserter(temp_vector));
                context->result_index_container.pop_back();
                context->result_index_container.emplace_back(temp_vector);
              } else {
                context->result_index_container.emplace_back(result);
              }
            } else {
              sort(result_rid.begin(), result_rid.end(), RowidCompare());
              if (context->is_and) {
                vector<RowId> temp_vector;
                set_intersection(context->result_rids_container.back().begin(),
                                 context->result_rids_container.back().end(), result_rid.begin(), result_rid.end(),
                                 back_inserter(temp_vector), RowidCompare());
                context->result_rids_container.pop_back();
                context->result_rids_container.emplace_back(temp_vector);
              } else {
                context->result_rids_container.emplace_back(result_rid);
              }
            }
          } else {
            if (context->is_and) {
              switch (type) {
                case kTypeInt: {
                  for (auto i : context->result_index_container.back()) {
                    if (context->value_i_of_col_[col_name][i] == "null") continue;
                    if (atoi(context->value_i_of_col_[col_name][i].c_str()) <= atoi(value.c_str())) {
                      result.emplace_back(i);
                    }
                  }
                  break;
                }
                case kTypeFloat: {
                  for (auto i : context->result_index_container.back()) {
                    if (context->value_i_of_col_[col_name][i] == "null") continue;
                    if (atof(context->value_i_of_col_[col_name][i].c_str()) <= atof(value.c_str())) {
                      result.emplace_back(i);
                    }
                  }
                  break;
                }
                case kTypeChar: {
                  for (auto i : context->result_index_container.back()) {
                    if (context->value_i_of_col_[col_name][i] == "null") continue;
                    if (context->value_i_of_col_[col_name][i] <= value) {
                      result.emplace_back(i);
                    }
                  }
                  break;
                }
                default:
                  break;
              }
              context->result_index_container.pop_back();
              context->result_index_container.emplace_back(result);
            } else {
              switch (type) {
                case kTypeInt: {
                  for (uint i = 0; i < context->value_i_of_col_[col_name].size(); i++) {
                    if (context->value_i_of_col_[col_name][i] == "null") continue;
                    if (atoi(context->value_i_of_col_[col_name][i].c_str()) <= atoi(value.c_str())) {
                      result.emplace_back(i);
                    }
                  }
                  break;
                }
                case kTypeFloat: {
                  for (uint i = 0; i < context->value_i_of_col_[col_name].size(); i++) {
                    if (context->value_i_of_col_[col_name][i] == "null") continue;
                    if (atof(context->value_i_of_col_[col_name][i].c_str()) <= atof(value.c_str())) {
                      result.emplace_back(i);
                    }
                  }
                  break;
                }
                case kTypeChar: {
                  for (uint i = 0; i < context->value_i_of_col_[col_name].size(); i++) {
                    if (context->value_i_of_col_[col_name][i] == "null") continue;
                    if (context->value_i_of_col_[col_name][i] <= value) {
                      result.emplace_back(i);
                    }
                  }
                  break;
                }
                default:
                  break;
              }
              context->result_index_container.emplace_back(result);
            }
          }
        } else if (compare_operator == ">") {
          if (context->index_on_one_key.find(col_name) != context->index_on_one_key.end()) {
            vector<Field> fields;
            switch (type) {
              case kTypeInt:
                fields.emplace_back(Field(type, atoi(value.c_str())));
                break;
              case kTypeFloat:
                fields.emplace_back(Field(type, (float)atof(value.c_str())));
                break;
              case kTypeChar: {
                char str[value.length() + 1];
                strcpy(str, value.c_str());
                fields.emplace_back(Field(type, str, value.length(), false));
                break;
              }
              default:
                break;
            }

            vector<RowId> result_rid;
            context->index_on_one_key[col_name]->GetIndex()->ScanKey(Row(fields), result_rid, context->txn_, ">");
            if (!context->index_only) {
              for (auto rid : result_rid) {
                result.emplace_back(context->rowid2index_map[rid.Get()]);
              }
              sort(result.begin(), result.end());
              if (context->is_and) {
                vector<uint> temp_vector;
                set_intersection(context->result_index_container.back().begin(),
                                 context->result_index_container.back().end(), result.begin(), result.end(),
                                 back_inserter(temp_vector));
                context->result_index_container.pop_back();
                context->result_index_container.emplace_back(temp_vector);
              } else {
                context->result_index_container.emplace_back(result);
              }
            } else {
              sort(result_rid.begin(), result_rid.end(), RowidCompare());
              if (context->is_and) {
                vector<RowId> temp_vector;
                set_intersection(context->result_rids_container.back().begin(),
                                 context->result_rids_container.back().end(), result_rid.begin(), result_rid.end(),
                                 back_inserter(temp_vector), RowidCompare());
                context->result_rids_container.pop_back();
                context->result_rids_container.emplace_back(temp_vector);
              } else {
                context->result_rids_container.emplace_back(result_rid);
              }
            }
          } else {
            if (context->is_and) {
              switch (type) {
                case kTypeInt: {
                  for (auto i : context->result_index_container.back()) {
                    if (context->value_i_of_col_[col_name][i] == "null") continue;
                    if (atoi(context->value_i_of_col_[col_name][i].c_str()) > atoi(value.c_str())) {
                      result.emplace_back(i);
                    }
                  }
                  break;
                }
                case kTypeFloat: {
                  for (auto i : context->result_index_container.back()) {
                    if (context->value_i_of_col_[col_name][i] == "null") continue;
                    if (atof(context->value_i_of_col_[col_name][i].c_str()) > atof(value.c_str())) {
                      result.emplace_back(i);
                    }
                  }
                  break;
                }
                case kTypeChar: {
                  for (auto i : context->result_index_container.back()) {
                    if (context->value_i_of_col_[col_name][i] == "null") continue;
                    if (context->value_i_of_col_[col_name][i] > value) {
                      result.emplace_back(i);
                    }
                  }
                  break;
                }
                default:
                  break;
              }
              context->result_index_container.pop_back();
              context->result_index_container.emplace_back(result);
            } else {
              switch (type) {
                case kTypeInt: {
                  for (uint i = 0; i < context->value_i_of_col_[col_name].size(); i++) {
                    if (context->value_i_of_col_[col_name][i] == "null") continue;
                    if (atoi(context->value_i_of_col_[col_name][i].c_str()) > atoi(value.c_str())) {
                      result.emplace_back(i);
                    }
                  }
                  break;
                }
                case kTypeFloat: {
                  for (uint i = 0; i < context->value_i_of_col_[col_name].size(); i++) {
                    if (context->value_i_of_col_[col_name][i] == "null") continue;
                    if (atof(context->value_i_of_col_[col_name][i].c_str()) > atof(value.c_str())) {
                      result.emplace_back(i);
                    }
                  }
                  break;
                }
                case kTypeChar: {
                  for (uint i = 0; i < context->value_i_of_col_[col_name].size(); i++) {
                    if (context->value_i_of_col_[col_name][i] == "null") continue;
                    if (context->value_i_of_col_[col_name][i] > value) {
                      result.emplace_back(i);
                    }
                  }
                  break;
                }
                default:
                  break;
              }
              context->result_index_container.emplace_back(result);
            }
          }
        } else if (compare_operator == ">=") {
          if (context->index_on_one_key.find(col_name) != context->index_on_one_key.end()) {
            vector<Field> fields;
            switch (type) {
              case kTypeInt:
                fields.emplace_back(Field(type, atoi(value.c_str())));
                break;
              case kTypeFloat:
                fields.emplace_back(Field(type, (float)atof(value.c_str())));
                break;
              case kTypeChar: {
                char str[value.length() + 1];
                strcpy(str, value.c_str());
                fields.emplace_back(Field(type, str, value.length(), false));
                break;
              }
              default:
                break;
            }

            vector<RowId> result_rid;
            context->index_on_one_key[col_name]->GetIndex()->ScanKey(Row(fields), result_rid, context->txn_, ">=");
            if (!context->index_only) {
              for (auto rid : result_rid) {
                result.emplace_back(context->rowid2index_map[rid.Get()]);
              }
              sort(result.begin(), result.end());
              if (context->is_and) {
                vector<uint> temp_vector;
                set_intersection(context->result_index_container.back().begin(),
                                 context->result_index_container.back().end(), result.begin(), result.end(),
                                 back_inserter(temp_vector));
                context->result_index_container.pop_back();
                context->result_index_container.emplace_back(temp_vector);
              } else {
                context->result_index_container.emplace_back(result);
              }
            } else {
              sort(result_rid.begin(), result_rid.end(), RowidCompare());
              if (context->is_and) {
                vector<RowId> temp_vector;
                set_intersection(context->result_rids_container.back().begin(),
                                 context->result_rids_container.back().end(), result_rid.begin(), result_rid.end(),
                                 back_inserter(temp_vector), RowidCompare());
                context->result_rids_container.pop_back();
                context->result_rids_container.emplace_back(temp_vector);
              } else {
                context->result_rids_container.emplace_back(result_rid);
              }
            }
          } else {
            if (context->is_and) {
              switch (type) {
                case kTypeInt: {
                  for (auto i : context->result_index_container.back()) {
                    if (context->value_i_of_col_[col_name][i] == "null") continue;
                    if (atoi(context->value_i_of_col_[col_name][i].c_str()) >= atoi(value.c_str())) {
                      result.emplace_back(i);
                    }
                  }
                  break;
                }
                case kTypeFloat: {
                  for (auto i : context->result_index_container.back()) {
                    if (context->value_i_of_col_[col_name][i] == "null") continue;
                    if (atof(context->value_i_of_col_[col_name][i].c_str()) >= atof(value.c_str())) {
                      result.emplace_back(i);
                    }
                  }
                  break;
                }
                case kTypeChar: {
                  for (auto i : context->result_index_container.back()) {
                    if (context->value_i_of_col_[col_name][i] == "null") continue;
                    if (context->value_i_of_col_[col_name][i] >= value) {
                      result.emplace_back(i);
                    }
                  }
                  break;
                }
                default:
                  break;
              }
              context->result_index_container.pop_back();
              context->result_index_container.emplace_back(result);
            } else {
              switch (type) {
                case kTypeInt: {
                  for (uint i = 0; i < context->value_i_of_col_[col_name].size(); i++) {
                    if (context->value_i_of_col_[col_name][i] == "null") continue;
                    if (atoi(context->value_i_of_col_[col_name][i].c_str()) >= atoi(value.c_str())) {
                      result.emplace_back(i);
                    }
                  }
                  break;
                }
                case kTypeFloat: {
                  for (uint i = 0; i < context->value_i_of_col_[col_name].size(); i++) {
                    if (context->value_i_of_col_[col_name][i] == "null") continue;
                    if (atof(context->value_i_of_col_[col_name][i].c_str()) >= atof(value.c_str())) {
                      result.emplace_back(i);
                    }
                  }
                  break;
                }
                case kTypeChar: {
                  for (uint i = 0; i < context->value_i_of_col_[col_name].size(); i++) {
                    if (context->value_i_of_col_[col_name][i] == "null") continue;
                    if (context->value_i_of_col_[col_name][i] >= value) {
                      result.emplace_back(i);
                    }
                  }
                  break;
                }
                default:
                  break;
              }
              context->result_index_container.emplace_back(result);
            }
          }
        } else if (compare_operator == "=") {
          if (context->index_on_one_key.find(col_name) != context->index_on_one_key.end()) {
            vector<Field> fields;
            switch (type) {
              case kTypeInt:
                fields.emplace_back(Field(type, atoi(value.c_str())));
                break;
              case kTypeFloat:
                fields.emplace_back(Field(type, (float)atof(value.c_str())));
                break;
              case kTypeChar: {
                char str[value.length() + 1];
                strcpy(str, value.c_str());
                fields.emplace_back(Field(type, str, value.length(), false));
                break;
              }
              default:
                break;
            }

            vector<RowId> result_rid;
            context->index_on_one_key[col_name]->GetIndex()->ScanKey(Row(fields), result_rid, context->txn_, "=");
            if (!context->index_only) {
              for (auto rid : result_rid) {
                result.emplace_back(context->rowid2index_map[rid.Get()]);
              }
              sort(result.begin(), result.end());
              if (context->is_and) {
                vector<uint> temp_vector;
                set_intersection(context->result_index_container.back().begin(),
                                 context->result_index_container.back().end(), result.begin(), result.end(),
                                 back_inserter(temp_vector));
                context->result_index_container.pop_back();
                context->result_index_container.emplace_back(temp_vector);
              } else {
                context->result_index_container.emplace_back(result);
              }
            } else {
              sort(result_rid.begin(), result_rid.end(), RowidCompare());
              if (context->is_and) {
                vector<RowId> temp_vector;
                set_intersection(context->result_rids_container.back().begin(),
                                 context->result_rids_container.back().end(), result_rid.begin(), result_rid.end(),
                                 back_inserter(temp_vector), RowidCompare());
                context->result_rids_container.pop_back();
                context->result_rids_container.emplace_back(temp_vector);
              } else {
                context->result_rids_container.emplace_back(result_rid);
              }
            }
          } else {
            if (context->is_and) {
              switch (type) {
                case kTypeInt: {
                  for (auto i : context->result_index_container.back()) {
                    if (context->value_i_of_col_[col_name][i] == "null") continue;
                    if (atoi(context->value_i_of_col_[col_name][i].c_str()) == atoi(value.c_str())) {
                      result.emplace_back(i);
                    }
                  }
                  break;
                }
                case kTypeFloat: {
                  for (auto i : context->result_index_container.back()) {
                    if (context->value_i_of_col_[col_name][i] == "null") continue;
                    if (atof(context->value_i_of_col_[col_name][i].c_str()) == atof(value.c_str())) {
                      result.emplace_back(i);
                    }
                  }
                  break;
                }
                case kTypeChar: {
                  for (auto i : context->result_index_container.back()) {
                    if (context->value_i_of_col_[col_name][i] == "null") continue;
                    if (context->value_i_of_col_[col_name][i] == value) {
                      result.emplace_back(i);
                    }
                  }
                  break;
                }
                default:
                  break;
              }
              context->result_index_container.pop_back();
              context->result_index_container.emplace_back(result);
            } else {
              switch (type) {
                case kTypeInt: {
                  for (uint i = 0; i < context->value_i_of_col_[col_name].size(); i++) {
                    if (context->value_i_of_col_[col_name][i] == "null") continue;
                    if (atoi(context->value_i_of_col_[col_name][i].c_str()) == atoi(value.c_str())) {
                      result.emplace_back(i);
                    }
                  }
                  break;
                }
                case kTypeFloat: {
                  for (uint i = 0; i < context->value_i_of_col_[col_name].size(); i++) {
                    if (context->value_i_of_col_[col_name][i] == "null") continue;
                    if (atof(context->value_i_of_col_[col_name][i].c_str()) == atof(value.c_str())) {
                      result.emplace_back(i);
                    }
                  }
                  break;
                }
                case kTypeChar: {
                  for (uint i = 0; i < context->value_i_of_col_[col_name].size(); i++) {
                    if (context->value_i_of_col_[col_name][i] == "null") continue;
                    if (context->value_i_of_col_[col_name][i] == value) {
                      result.emplace_back(i);
                    }
                  }
                  break;
                }
                default:
                  break;
              }
              context->result_index_container.emplace_back(result);
            }
          }
        } else if (compare_operator == "<>") {
          if (context->index_on_one_key.find(col_name) != context->index_on_one_key.end()) {
            vector<Field> fields;
            switch (type) {
              case kTypeInt:
                fields.emplace_back(Field(type, atoi(value.c_str())));
                break;
              case kTypeFloat:
                fields.emplace_back(Field(type, (float)atof(value.c_str())));
                break;
              case kTypeChar: {
                char str[value.length() + 1];
                strcpy(str, value.c_str());
                fields.emplace_back(Field(type, str, value.length(), false));
                break;
              }
              default:
                break;
            }

            vector<RowId> result_rid;
            if (!context->index_only) {
              context->index_on_one_key[col_name]->GetIndex()->ScanKey(Row(fields), result_rid, context->txn_, "=");
              if (context->is_and) {
                if (context->rowid2index_map.find(result_rid[0].Get()) != context->rowid2index_map.end()) {
                  context->result_index_container.back().erase(find(context->result_index_container.back().begin(),
                                                                    context->result_index_container.back().end(),
                                                                    context->rowid2index_map[result_rid[0].Get()]));
                }
              } else {
                if (!result_rid.empty()) {
                  for (auto rid_index_pair : context->rowid2index_map) {
                    if (rid_index_pair.first != result_rid[0].Get()) result.emplace_back(rid_index_pair.second);
                  }
                } else {
                  for (auto rid_index_pair : context->rowid2index_map) {
                    result.emplace_back(rid_index_pair.second);
                  }
                }
                sort(result.begin(), result.end());
                context->result_index_container.emplace_back(result);
              }
            } else {
              context->index_on_one_key[col_name]->GetIndex()->ScanKey(Row(fields), result_rid, context->txn_, "<>");
              sort(result_rid.begin(), result_rid.end(), RowidCompare());
              if (context->is_and) {
                vector<RowId> temp_vector;
                set_intersection(context->result_rids_container.back().begin(),
                                 context->result_rids_container.back().end(), result_rid.begin(), result_rid.end(),
                                 back_inserter(temp_vector), RowidCompare());
                context->result_rids_container.pop_back();
                context->result_rids_container.emplace_back(temp_vector);
              } else {
                context->result_rids_container.emplace_back(result_rid);
              }
            }
          } else {
            if (context->is_and) {
              switch (type) {
                case kTypeInt: {
                  for (auto i : context->result_index_container.back()) {
                    if (context->value_i_of_col_[col_name][i] == "null") continue;
                    if (atoi(context->value_i_of_col_[col_name][i].c_str()) != atoi(value.c_str())) {
                      result.emplace_back(i);
                    }
                  }
                  break;
                }
                case kTypeFloat: {
                  for (auto i : context->result_index_container.back()) {
                    if (context->value_i_of_col_[col_name][i] == "null") continue;
                    if (atof(context->value_i_of_col_[col_name][i].c_str()) != atof(value.c_str())) {
                      result.emplace_back(i);
                    }
                  }
                  break;
                }
                case kTypeChar: {
                  for (auto i : context->result_index_container.back()) {
                    if (context->value_i_of_col_[col_name][i] == "null") continue;
                    if (context->value_i_of_col_[col_name][i] != value) {
                      result.emplace_back(i);
                    }
                  }
                  break;
                }
                default:
                  break;
              }
              context->result_index_container.pop_back();
              context->result_index_container.emplace_back(result);
            } else {
              switch (type) {
                case kTypeInt: {
                  for (uint i = 0; i < context->value_i_of_col_[col_name].size(); i++) {
                    if (context->value_i_of_col_[col_name][i] == "null") continue;
                    if (atoi(context->value_i_of_col_[col_name][i].c_str()) != atoi(value.c_str())) {
                      result.emplace_back(i);
                    }
                  }
                  break;
                }
                case kTypeFloat: {
                  for (uint i = 0; i < context->value_i_of_col_[col_name].size(); i++) {
                    if (context->value_i_of_col_[col_name][i] == "null") continue;
                    if (atof(context->value_i_of_col_[col_name][i].c_str()) != atof(value.c_str())) {
                      result.emplace_back(i);
                    }
                  }
                  break;
                }
                case kTypeChar: {
                  for (uint i = 0; i < context->value_i_of_col_[col_name].size(); i++) {
                    if (context->value_i_of_col_[col_name][i] == "null") continue;
                    if (context->value_i_of_col_[col_name][i] != value) {
                      result.emplace_back(i);
                    }
                  }
                  break;
                }
                default:
                  break;
              }
              context->result_index_container.emplace_back(result);
            }
          }
        } else {
          throw invalid_argument("Unknown compare operator");
        }
      }
      break;
    }
    case kNodeUpdateValue:{
      string col_name = ast->child_->val_;
      string value = ast->child_->next_->val_;
      bool is_exist = false;
      for (auto col : context->columns) {
        if (col->GetName() == col_name) {
          uint col_idx = col->GetTableInd();
          TypeId type = col->GetType();
          is_exist = true;
          switch(type){
            case kTypeInt: context->update_field_map.emplace(col_idx, Field(type, atoi(value.c_str()))); break;
            case kTypeFloat: context->update_field_map.emplace(col_idx, Field(type, (float)atof(value.c_str()))); break;
            case kTypeChar: {
              char str[value.length() + 1];
              strcpy(str, value.c_str());
              context->update_field_map.emplace(col_idx, Field(type, str, value.length(), true));
              break;
            }
            default: break;
          }
          break;
        }
      }
      if (!is_exist) {
        string error_info = " Unknown column '";
        error_info += col_name;
        error_info += "' in 'field list'";
        throw invalid_argument(error_info);
      }
      break;
    }
    default: {
      if (ast->child_ != nullptr) Next(ast->child_, context);
      if (ast->next_ != nullptr) Next(ast->next_, context);
      break;
    }
  }
}

  dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  context->print_flag = 0;
  if(!context->dont_print) gettimeofday(&context->start, nullptr);
  context->DBname = ast->child_->val_;
  if (context->DBname != nullptr) {
    if (dbs_.find(context->DBname) == dbs_.end()) {
      auto *database = new DBStorageEngine(context->DBname);
      dbs_.emplace(pair(context->DBname, database));
      context->rows_num++;
      PrintInfo(context);
      return DB_SUCCESS;
    } else {
      cout << "Can't create database '" << context->DBname << "'; database exists" << endl;
      return DB_FAILED;
    }
  } else {
    cout << "Unknown error" << endl;
    return DB_FAILED;
  }
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  context->print_flag = 0;
  if(!context->dont_print) gettimeofday(&context->start, NULL);
  context->DBname = ast->child_->val_;
  if (context->DBname != nullptr) {
    if (dbs_.find(context->DBname) != dbs_.end()) {
      string deleted_file = "./databases/";
      deleted_file += context->DBname;
      remove(deleted_file.c_str());
      dbs_.erase(context->DBname);
      context->rows_num++;
      PrintInfo(context);
      return DB_SUCCESS;
    } else {
      cout << "Can't drop database '" << context->DBname << "'; database doesn't exist" << endl;
      return DB_FAILED;
    }
  } else {
    cout << "Unknown error" << endl;
    return DB_FAILED;
  }
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  if(dbs_.empty()) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_FAILED;
  }
  context->print_flag = 1;
  if(!context->dont_print) gettimeofday(&context->start, nullptr);
  uint max_width = 8;
  for(const auto& itr : dbs_){
    if(itr.first.length() > max_width) max_width = itr.first.length();
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << "" << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_width) << "Database" << " |"<< endl;
  cout << "+" << setfill('-') << setw(max_width + 2) << "" << "+" << endl;
  for(const auto& itr : dbs_){
    cout << "| " << std::left << setfill(' ') << setw(max_width) << itr.first << " |"<< endl;
    context->rows_num++;
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << "" << "+" << endl;
  PrintInfo(context);
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  context->DBname = ast->child_->val_;
  if (context->DBname != nullptr) {
    if (dbs_.find(context->DBname) != dbs_.end()) {
      current_db_ = context->DBname;
      cout << "Database changed" << endl;
      return DB_SUCCESS;
    } else {
      cout << "Unknown database '" << context->DBname << "'" << endl;
      return DB_FAILED;
    }
  } else {
    cout << "Unknown error" << endl;
    return DB_FAILED;
  }
}

dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  if(current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  context->print_flag = 1;
  if(!context->dont_print)  gettimeofday(&context->start, NULL);
  vector<TableInfo*> tables;
  if(dbs_[current_db_]->catalog_mgr_->GetTables(tables) == DB_FAILED) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_FAILED;
  }
  string table_in_db("Tables_in_" + current_db_);
  uint max_width = table_in_db.length();
  for(const auto& itr : tables){
    if(itr->GetTableName().length() > max_width) max_width = itr->GetTableName().length();
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << "" << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_width) << table_in_db << " |"<< endl;
  cout << "+" << setfill('-') << setw(max_width + 2) << "" << "+" << endl;
  for(const auto& itr : tables){
    cout << "| " << std::left << setfill(' ') << setw(max_width) << itr->GetTableName() << " |"<< endl;
    context->rows_num++;
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << "" << "+" << endl;
  PrintInfo(context);
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  if(current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  context->print_flag = 0;
  if(!context->dont_print) gettimeofday(&context->start, NULL);
  context->table_name = ast->child_->val_;
  try{
    Next(ast->child_, context);
  }catch(exception &error_info){
    cout << error_info.what() << endl;
    for(auto column : context->columns) { delete column; }
    return DB_FAILED;
  }
  TableInfo *table_info;
  Schema schema(context->columns);
  dberr_t flag = dbs_[current_db_]->catalog_mgr_->CreateTable(context->table_name, &schema, nullptr,
                                                              table_info, context->key_map);
  switch (flag) {
    case DB_TABLE_ALREADY_EXIST:{
      cout << "Table '"<< context->table_name <<"' already exists" << endl;
      for(auto column : context->columns) { delete column; }
      return DB_FAILED;
    }
    case DB_SUCCESS:{
      context->rows_num++;
      IndexInfo* index_info;
      if(!context->key_map.empty()) {
        string index_name = context->table_name;
        index_name += ".PRIMARY";
        dbs_[current_db_]->catalog_mgr_->CreateIndex(context->table_name, index_name, context->key_map,
                                                     nullptr, index_info, context->index_type);
        context->rows_num++;
      }
      /*for(auto i : context->unique_col){
        string index_name = context->table_name;
        index_name += ".KEY_ON_'";
        index_name += schema.GetColumns()[i]->GetName();
        index_name += "'";
        dbs_[current_db_]->catalog_mgr_->CreateIndex(context->table_name, index_name, vector<uint>{i},
                                                     nullptr, index_info, context->index_type);
        context->rows_num++;
      }*/
      PrintInfo(context);
      for(auto column : context->columns) { delete column; }
      return DB_SUCCESS;
    }
    default:
      for(auto column : context->columns) { delete column; }
      return DB_FAILED;
  }
}

dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  if(current_db_.empty()) {
  cout << "No database selected" << endl;
  return DB_FAILED;
  }
  context->print_flag = 0;
  if(!context->dont_print) gettimeofday(&context->start, nullptr);
  context->table_name = ast->child_->val_;
  if (context->table_name != nullptr) {
    dberr_t flag = dbs_[current_db_]->catalog_mgr_->DropTable(context->table_name);
    if(flag == DB_TABLE_NOT_EXIST){
      cout << "Table '"<< context->table_name <<"' doesn't exist" << endl;
      return DB_FAILED;
    }else if(flag == DB_SUCCESS){
      context->rows_num++;
      PrintInfo(context);
      return DB_SUCCESS;
    }
  }
  cout << "Unknown error" << endl;
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  if(current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  context->print_flag = 1;
  if(!context->dont_print) gettimeofday(&context->start, NULL);
  vector<TableInfo*> tables;
  if(dbs_[current_db_]->catalog_mgr_->GetTables(tables) == DB_FAILED) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_FAILED;
  }
  vector<vector<IndexInfo *>> indexes_of_table_;
  vector<uint> index_num_of_table_;
  uint total_rows = 0;
  for(uint i = 0; i < tables.size(); i++) {
    indexes_of_table_.emplace_back(vector<IndexInfo *>());
    dbs_[current_db_]->catalog_mgr_->GetTableIndexes(tables[i]->GetTableName(), indexes_of_table_[i]);
    total_rows += indexes_of_table_[i].size();
    index_num_of_table_.emplace_back(indexes_of_table_[i].size());
  }
  if(total_rows == 0) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_FAILED;
  }
  vector<string> header{"table_name", "index_name"};
  vector<vector<string>> output_info;
  output_info.emplace_back(vector<string>());
  output_info.emplace_back(vector<string>());
  for(uint i = 0; i < index_num_of_table_.size(); i++) {
    for(uint j = 0; j < index_num_of_table_[i]; j++){
      output_info[0].emplace_back(tables[i]->GetTableName());
      output_info[1].emplace_back(indexes_of_table_[i][j]->GetIndexName());
    }
  }
  PrintTable(header, output_info);
  context->rows_num = total_rows;
  PrintInfo(context);
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  if(current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  context->print_flag = 0;
  if(!context->dont_print) gettimeofday(&context->start, NULL);
  context->index_name = ast->child_->val_;
  context->table_name = ast->child_->next_->val_;
  TableInfo* table_info;
  if(dbs_[current_db_]->catalog_mgr_->GetTable(context->table_name, table_info) == DB_TABLE_NOT_EXIST) {
    cout << "Table '"<< context->table_name <<"' doesn't exist" << endl;
    return DB_FAILED;
  }
  context->columns = table_info->GetSchema()->GetColumns();
  try{
    Next(ast->child_->next_->next_, context);
  }catch(exception &error_info){
    cout << error_info.what() << endl;
    return DB_FAILED;
  }
  bool is_unique{false};
  bool flag = false;
  string error_info("(");
  for(uint ind : context->key_map){
    if(flag) error_info += ", ";
    else flag = true;
    if(context->columns[ind]->IsUnique()) is_unique = true;
    error_info += context->columns[ind]->GetName();
  }
  error_info += ") is not unique, so you can't create index on it";
  if(!is_unique){
    cout << error_info << endl;
    return DB_FAILED;
  }
  IndexInfo* index_info;

  dberr_t error_flag = dbs_[current_db_]->catalog_mgr_->CreateIndex(
      context->table_name, context->index_name, context->key_map, nullptr, index_info, context->index_type);

  switch (error_flag) {
    case DB_TABLE_NOT_EXIST:{
      cout << "Table '"<< context->table_name <<"' doesn't exist" << endl;
      return DB_FAILED;
    }
    case DB_INDEX_ALREADY_EXIST:{
      cout << "Index '"<< context->index_name <<"' of table '"<< context->table_name << "' already exists" << endl;
      return DB_FAILED;
    }
    case DB_SUCCESS:{
      context->rows_num++;
      PrintInfo(context);
      return DB_SUCCESS;
    }
    default:
      return DB_FAILED;
  }
}

dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  if(current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  context->print_flag = 0;
  if(!context->dont_print) gettimeofday(&context->start, NULL);
  vector<TableInfo*> tables;
  if(dbs_[current_db_]->catalog_mgr_->GetTables(tables) == DB_FAILED) {
    return DB_FAILED;
  }
  string index_name(ast->child_->val_);
  for(auto table : tables){
    if(dbs_[current_db_]->catalog_mgr_->DropIndex(table->GetTableName(), index_name) == DB_SUCCESS)
      context->rows_num++;
  }
  if(context->rows_num > 0){
    PrintInfo(context);
    return DB_SUCCESS;
  }else{
    cout << "Index '"<< context->index_name <<"' doesn't exist" << endl;
    return DB_FAILED;
  }
}

dberr_t ExecuteEngine::ExecuteSelect(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteSelect" << std::endl;
#endif
  if(current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  context->print_flag = 1;
  if(!context->dont_print) gettimeofday(&context->start, nullptr);
  context->table_name = ast->child_->next_->val_;
  TableInfo* table_info;
  if(dbs_[current_db_]->catalog_mgr_->GetTable(context->table_name, table_info) == DB_TABLE_NOT_EXIST) {
    cout << "Table '"<< context->table_name <<"' doesn't exist" << endl;
    return DB_FAILED;
  }
  vector<IndexInfo *> indexes;
  dbs_[current_db_]->catalog_mgr_->GetTableIndexes(context->table_name, indexes);
  for(auto index : indexes){
    if(index->GetIndexKeySchema()->GetColumns().size() == 1){
      context->index_on_one_key[index->GetIndexKeySchema()->GetColumn(0)->GetName()] = index;
    }
  }
  vector<string> column_name;
  context->columns = table_info->GetSchema()->GetColumns();
  for (auto col : context->columns) {
    context->value_i_of_col_[col->GetName()] = vector<string>{};
    column_name.emplace_back(col->GetName());
  }
  FindConditionColumn(ast, context);
  if(!context->has_condition){
    context->index_only = false;
  }else{
    for (const auto &colname : context->condition_colname) {
      if (context->index_on_one_key.find(colname) == context->index_on_one_key.end()) context->index_only = false;
    }
  }
  if(!context->index_only){
    uint i = 0;
    for (auto iter = table_info->GetTableHeap()->Begin(); iter != table_info->GetTableHeap()->End(); iter++) {
      for (uint j = 0; j < context->value_i_of_col_.size(); ++j) {
        if (iter->GetField(j)->IsNull()) {
          context->value_i_of_col_[column_name[j]].emplace_back("null");
        } else {
          context->value_i_of_col_[column_name[j]].emplace_back(iter->GetField(j)->GetFieldValue());
        }
      }
      // context->index2rowid_map[i] = iter.GetRowId_64();
      context->rowid2index_map[iter.GetRowId_64()] = i++;
    }
    try{
      Next(ast, context);
    }catch(exception &error_info){
      cout << error_info.what() << endl;
      return DB_FAILED;
    }
  }else{
    try{
      Next(ast, context);
    }catch(exception &error_info){
      cout << error_info.what() << endl;
      return DB_FAILED;
    }
    vector<RowId> final_result_rids{};
    for(auto container : context->result_rids_container) {
      vector<RowId> temp;
      sort(container.begin(), container.end(), RowidCompare());
      set_union(container.begin(), container.end(), final_result_rids.begin(), final_result_rids.end(),
                back_inserter(temp), RowidCompare());
      final_result_rids = temp;
    }
    for(auto rid : final_result_rids) {
      Row row(rid);
      table_info->GetTableHeap()->GetTuple(&row, context->txn_);
      for (uint j = 0; j < context->value_i_of_col_.size(); ++j) {
        if (row.GetField(j)->IsNull()) {
          context->value_i_of_col_[column_name[j]].emplace_back("null");
        } else {
          context->value_i_of_col_[column_name[j]].emplace_back(row.GetField(j)->GetFieldValue());
        }
      }
    }
  }
  vector<string> header;
  vector<vector<string>> output_result;
  if (!context->key_map.empty()) {
    for (auto col_idx : context->key_map) {
      header.emplace_back(column_name[col_idx]);
      output_result.emplace_back(context->value_i_of_col_[column_name[col_idx]]);
    }
  } else {
    for (const auto& col : column_name) {
      header.emplace_back(col);
      output_result.emplace_back(context->value_i_of_col_[col]);
    }
  }
  if(context->has_condition && !context->index_only){
    vector<uint32_t> final_result_index{};
    for(auto container : context->result_index_container) {
      vector<uint32_t> temp;
      sort(container.begin(), container.end());
      set_union(container.begin(), container.end(), final_result_index.begin(), final_result_index.end(),
                back_inserter(temp));
      final_result_index = temp;
    }
    context->rows_num += final_result_index.size();
    if(context->rows_num != 0){
      PrintTable(header, output_result, final_result_index);
    }
  }else{
    context->rows_num += output_result[0].size();
    if(context->rows_num != 0) {
      PrintTable(header, output_result);
    }
  }
  PrintInfo(context);
  return DB_SUCCESS;
}


dberr_t ExecuteEngine::ExecuteInsert(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteInsert" << std::endl;
#endif
  if(current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  context->print_flag = 0;
  if(!context->dont_print) gettimeofday(&context->start, nullptr);
  string table_name = ast->child_->val_;
  TableInfo* table_info;
  vector<IndexInfo *> indexes;
  if(dbs_[current_db_]->catalog_mgr_->GetTable(table_name, table_info) == DB_TABLE_NOT_EXIST) {
    cout << "Table '"<< context->table_name <<"' doesn't exist" << endl;
    return DB_FAILED;
  }
  dbs_[current_db_]->catalog_mgr_->GetTableIndexes(table_name, indexes);
  ast = ast->child_->next_->child_;
  vector<Field> fields;
  for(auto col : table_info->GetSchema()->GetColumns()){
    if(ast == nullptr){
      cout << " Column count doesn't match value count" << endl;
      return DB_FAILED;
    }else if(ast->type_ == kNodeNull){
      if(col->IsNullable() && !col->IsUnique()){
        fields.emplace_back(Field(col->GetType()));
      }else{
        cout << "Field '" << col->GetName() << "' can't be null" << endl;
        return DB_FAILED;
      }
    }else{
      string value = ast->val_;
      if(col->GetType() == kTypeInt && ast->type_ == kNodeNumber){//类型检查
        int val_int = atoi(value.c_str());
        fields.emplace_back(Field(col->GetType(), val_int));
      }else if(col->GetType() == kTypeFloat && ast->type_ == kNodeNumber){
        float val_float = atof(value.c_str());
        fields.emplace_back(Field(col->GetType(), val_float));
      }else if(col->GetType() == kTypeChar && ast->type_ == kNodeString){
        if(value.length() > col->GetLength()){
          cout << "Data too long for column '" << col->GetName() << "'" << endl;
          return DB_FAILED;
        }else{
          char *val_char = new char[value.length() + 1];
          strcpy(val_char, value.c_str());
          fields.emplace_back(Field(col->GetType(), val_char, value.length(), true));
          delete[] val_char;
        }
      }else{
        cout << "Wrong data type for column '" << col->GetName() << "'" << endl;
        return DB_FAILED;
      }
    }
    ast = ast->next_;
  }
  if(ast != nullptr){
    cout << " Column count doesn't match value count" << endl;
    return DB_FAILED;
  }
  Row row(fields);
  vector<vector<Field>> indexes_fields;
  for(auto index : indexes){
    vector<Field> index_fields;
    for(auto col : index->GetIndexKeySchema()->GetColumns()){
      index_fields.emplace_back(*row.GetFields()[col->GetTableInd()]);
    }
    indexes_fields.emplace_back(index_fields);
    vector<RowId> result;
    if(index->GetIndex()->ScanKey(Row(index_fields), result, nullptr) == DB_SUCCESS){
      string error_info = "Duplicate entry '";
      bool temp_flag = false;
      for(auto &filed : index_fields){
        if(temp_flag) error_info += "-";
        else temp_flag = true;
        error_info += filed.GetFieldValue();
      }
      error_info += "' for key '";
      error_info += index->GetIndexName();
      error_info += "'";
      cout << error_info << endl;
      return DB_FAILED;
    }
  }
  if(table_info->GetTableHeap()->InsertTuple(row, nullptr)){
    for(uint i = 0; i < indexes.size(); i++){
      if(indexes[i]->GetIndex()->InsertEntry(Row(indexes_fields[i]), row.GetRowId(), nullptr) == DB_SUCCESS){
        context->rows_num++;
      }else{
        cout << "Unknown error" << endl;
        return DB_FAILED;
      }
    }
    context->rows_num++;
    PrintInfo(context);
    return DB_SUCCESS;
  }else{
    cout << "Unknown error" << endl;
    return DB_FAILED;
  }
}

dberr_t ExecuteEngine::ExecuteDelete(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDelete" << std::endl;
#endif
  if(current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  context->print_flag = 0;
  if(!context->dont_print) gettimeofday(&context->start, nullptr);
  context->table_name = ast->child_->val_;
  TableInfo* table_info;
  if(dbs_[current_db_]->catalog_mgr_->GetTable(context->table_name, table_info) == DB_TABLE_NOT_EXIST) {
    cout << "Table '"<< context->table_name <<"' doesn't exist" << endl;
    return DB_FAILED;
  }
  vector<IndexInfo *> indexes;
  dbs_[current_db_]->catalog_mgr_->GetTableIndexes(context->table_name, indexes);
  for(auto index : indexes){
    if(index->GetIndexKeySchema()->GetColumns().size() == 1){
      context->index_on_one_key[index->GetIndexKeySchema()->GetColumn(0)->GetName()] = index;
    }
  }
  vector<string> column_name;
  context->columns = table_info->GetSchema()->GetColumns();
  for(auto col : context->columns){
    context->value_i_of_col_[col->GetName()] = vector<string>{};
    column_name.emplace_back(col->GetName());
  }

  FindConditionColumn(ast, context);
  if(!context->has_condition){
    context->index_only = false;
    for (auto iter = table_info->GetTableHeap()->Begin(); iter != table_info->GetTableHeap()->End(); iter++) {
      Row row(iter->GetRowId());
      table_info->GetTableHeap()->GetTuple(&row, context->txn_);
      for(auto index : indexes){
        vector<Field> key_fields;
        for(auto col : index->GetIndexKeySchema()->GetColumns()){
          key_fields.emplace_back(*row.GetField(col->GetTableInd()));
        }
        index->GetIndex()->RemoveEntry(Row(key_fields), INVALID_ROWID, context->txn_);
        context->rows_num++;
      }
      table_info->GetTableHeap()->ApplyDelete(row.GetRowId(), context->txn_);
      context->rows_num++;
    }
  }else {
    for (const auto &colname : context->condition_colname) {
      if (context->index_on_one_key.find(colname) == context->index_on_one_key.end()) context->index_only = false;
    }
    if (!context->index_only) {
      uint i = 0;
      for (auto iter = table_info->GetTableHeap()->Begin(); iter != table_info->GetTableHeap()->End(); iter++) {
        for (uint j = 0; j < context->value_i_of_col_.size(); ++j) {
          if (iter->GetField(j)->IsNull()) {
            context->value_i_of_col_[column_name[j]].emplace_back("null");
          } else {
            context->value_i_of_col_[column_name[j]].emplace_back(iter->GetField(j)->GetFieldValue());
          }
        }
        context->index2rowid_map[i] = iter.GetRowId_64();
        context->rowid2index_map[iter.GetRowId_64()] = i++;
      }
      try {
        Next(ast->child_->next_, context);
      } catch (exception &error_info) {
        cout << error_info.what() << endl;
        return DB_FAILED;
      }
      vector<uint32_t> final_deleted_index{};
      for(auto container : context->result_index_container) {
        vector<uint32_t> temp;
        sort(container.begin(), container.end());
        set_union(container.begin(), container.end(), final_deleted_index.begin(), final_deleted_index.end(),
                  back_inserter(temp));
        final_deleted_index = temp;
      }
      for(auto idx : final_deleted_index){
        Row row(RowId(context->index2rowid_map[idx]));
        table_info->GetTableHeap()->GetTuple(&row, context->txn_);
        for(auto index : indexes){
          vector<Field> key_fields;
          for(auto col : index->GetIndexKeySchema()->GetColumns()){
            key_fields.emplace_back(*row.GetField(col->GetTableInd()));
          }
          index->GetIndex()->RemoveEntry(Row(key_fields), INVALID_ROWID, context->txn_);
          context->rows_num++;
        }
        table_info->GetTableHeap()->ApplyDelete(row.GetRowId(), context->txn_);
        context->rows_num++;
      }
    } else {
      try {
        Next(ast->child_->next_, context);
      } catch (exception &error_info) {
        cout << error_info.what() << endl;
        return DB_FAILED;
      }
      vector<RowId> final_delete_rids{};
      for (auto container : context->result_rids_container) {
        vector<RowId> temp;
        sort(container.begin(), container.end(), RowidCompare());
        set_union(container.begin(), container.end(), final_delete_rids.begin(), final_delete_rids.end(),
                  back_inserter(temp), RowidCompare());
        final_delete_rids = temp;
      }
      for(auto rid : final_delete_rids){
        Row row(rid);
        table_info->GetTableHeap()->GetTuple(&row, context->txn_);
        for(auto index : indexes){
          vector<Field> key_fields;
          for(auto col : index->GetIndexKeySchema()->GetColumns()){
            key_fields.emplace_back(*row.GetField(col->GetTableInd()));
          }
          index->GetIndex()->RemoveEntry(Row(key_fields), INVALID_ROWID, context->txn_);
          context->rows_num++;
        }
        table_info->GetTableHeap()->ApplyDelete(row.GetRowId(), context->txn_);
        context->rows_num++;
      }
    }
  }
  PrintInfo(context);
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUpdate(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUpdate" << std::endl;
#endif
  if(current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  context->print_flag = 0;
  if(!context->dont_print) gettimeofday(&context->start, nullptr);
  context->table_name = ast->child_->val_;
  TableInfo* table_info;
  if(dbs_[current_db_]->catalog_mgr_->GetTable(context->table_name, table_info) == DB_TABLE_NOT_EXIST) {
    cout << "Table '"<< context->table_name <<"' doesn't exist" << endl;
    return DB_FAILED;
  }
  vector<IndexInfo *> indexes;
  dbs_[current_db_]->catalog_mgr_->GetTableIndexes(context->table_name, indexes);
  for(auto index : indexes){
    if(index->GetIndexKeySchema()->GetColumns().size() == 1){
      context->index_on_one_key[index->GetIndexKeySchema()->GetColumn(0)->GetName()] = index;
    }
  }
  vector<string> column_name;
  context->columns = table_info->GetSchema()->GetColumns();
  for(auto col : context->columns){
    context->value_i_of_col_[col->GetName()] = vector<string>{};
    column_name.emplace_back(col->GetName());
  }

  FindConditionColumn(ast, context);
  if(!context->has_condition){
    context->index_only = false;
    try {
      Next(ast->child_->next_, context);
    } catch (exception &error_info) {
      cout << error_info.what() << endl;
      return DB_FAILED;
    }
    for (auto iter = table_info->GetTableHeap()->Begin(); iter != table_info->GetTableHeap()->End(); iter++) {
      Row old_row(iter->GetRowId());
      table_info->GetTableHeap()->GetTuple(&old_row, context->txn_);

      vector<Field> new_row_fields;
      for (uint i = 0; i < old_row.GetFieldCount(); ++i) {
        new_row_fields.emplace_back(*old_row.GetField(i));
      }
      for(auto idx_field_pair : context->update_field_map){
        new_row_fields[idx_field_pair.first] = idx_field_pair.second;
      }
      Row new_row(new_row_fields);

      for(auto index : indexes){
        vector<Field> old_key_fields;
        vector<Field> new_key_fields;
        for(auto col : index->GetIndexKeySchema()->GetColumns()){
          old_key_fields.emplace_back(*old_row.GetField(col->GetTableInd()));
          new_key_fields.emplace_back(new_row_fields[col->GetTableInd()]);
        }
        index->GetIndex()->RemoveEntry(Row(old_key_fields), INVALID_ROWID, context->txn_);
        index->GetIndex()->InsertEntry(Row(new_key_fields), old_row.GetRowId(), context->txn_);
        context->rows_num++;
      }

      table_info->GetTableHeap()->UpdateTuple(new_row, old_row.GetRowId(), context->txn_);
      context->rows_num++;
    }
  }else{
    for (const auto &colname : context->condition_colname) {
      if (context->index_on_one_key.find(colname) == context->index_on_one_key.end()) context->index_only = false;
    }
    if (!context->index_only) {
      uint i = 0;
      for (auto iter = table_info->GetTableHeap()->Begin(); iter != table_info->GetTableHeap()->End(); iter++) {
        for (uint j = 0; j < context->value_i_of_col_.size(); ++j) {
          if (iter->GetField(j)->IsNull()) {
            context->value_i_of_col_[column_name[j]].emplace_back("null");
          } else {
            context->value_i_of_col_[column_name[j]].emplace_back(iter->GetField(j)->GetFieldValue());
          }
        }
        context->index2rowid_map[i] = iter.GetRowId_64();
        context->rowid2index_map[iter.GetRowId_64()] = i++;
      }
      try {
        Next(ast->child_->next_, context);
      } catch (exception &error_info) {
        cout << error_info.what() << endl;
        return DB_FAILED;
      }
      vector<uint32_t> final_update_index{};
      for(auto container : context->result_index_container) {
        vector<uint32_t> temp;
        sort(container.begin(), container.end());
        set_union(container.begin(), container.end(), final_update_index.begin(), final_update_index.end(),
                  back_inserter(temp));
        final_update_index = temp;
      }
      for(auto idx : final_update_index){
        Row old_row(RowId(context->index2rowid_map[idx]));
        table_info->GetTableHeap()->GetTuple(&old_row, context->txn_);

        vector<Field> new_row_fields;
        for (uint j = 0; j < old_row.GetFieldCount(); ++j) {
          new_row_fields.emplace_back(*old_row.GetField(j));
        }
        for(auto idx_field_pair : context->update_field_map){
          new_row_fields[idx_field_pair.first] = idx_field_pair.second;
        }
        Row new_row(new_row_fields);

        for(auto index : indexes){
          vector<Field> old_key_fields;
          vector<Field> new_key_fields;
          for(auto col : index->GetIndexKeySchema()->GetColumns()){
            old_key_fields.emplace_back(*old_row.GetField(col->GetTableInd()));
            new_key_fields.emplace_back(new_row_fields[col->GetTableInd()]);
          }
          index->GetIndex()->RemoveEntry(Row(old_key_fields), INVALID_ROWID, context->txn_);
          index->GetIndex()->InsertEntry(Row(new_key_fields), old_row.GetRowId(), context->txn_);
          context->rows_num++;
        }

        table_info->GetTableHeap()->UpdateTuple(new_row, old_row.GetRowId(), context->txn_);
        context->rows_num++;
      }
    } else {
      try {
        Next(ast->child_->next_, context);
      } catch (exception &error_info) {
        cout << error_info.what() << endl;
        return DB_FAILED;
      }
      vector<RowId> final_update_rids{};
      for (auto container : context->result_rids_container) {
        vector<RowId> temp;
        sort(container.begin(), container.end(), RowidCompare());
        set_union(container.begin(), container.end(), final_update_rids.begin(), final_update_rids.end(),
                  back_inserter(temp), RowidCompare());
        final_update_rids = temp;
      }
      for(auto rid : final_update_rids){
        Row old_row(rid);
        table_info->GetTableHeap()->GetTuple(&old_row, context->txn_);

        vector<Field> new_row_fields;
        for (uint j = 0; j < old_row.GetFieldCount(); ++j) {
          new_row_fields.emplace_back(*old_row.GetField(j));
        }
        for(auto idx_field_pair : context->update_field_map){
          new_row_fields[idx_field_pair.first] = idx_field_pair.second;
        }
        Row new_row(new_row_fields);

        for(auto index : indexes){
          vector<Field> old_key_fields;
          vector<Field> new_key_fields;
          for(auto col : index->GetIndexKeySchema()->GetColumns()){
            old_key_fields.emplace_back(*old_row.GetField(col->GetTableInd()));
            new_key_fields.emplace_back(new_row_fields[col->GetTableInd()]);
          }
          index->GetIndex()->RemoveEntry(Row(old_key_fields), INVALID_ROWID, context->txn_);
          index->GetIndex()->InsertEntry(Row(new_key_fields), old_row.GetRowId(), context->txn_);
          context->rows_num++;
        }

        table_info->GetTableHeap()->UpdateTuple(new_row, old_row.GetRowId(), context->txn_);
        context->rows_num++;
      }
    }
  }
  PrintInfo(context);
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  context->print_flag = 0;
  if(!context->dont_print)  gettimeofday(&context->start, nullptr);
  string file_path = ast->child_->val_;
  FILE *fp = fopen(file_path.c_str(), "r");
  if (fp == nullptr)
  {
    cout << "Fail to read file" << endl;
    return DB_FAILED;
  }
  const int buf_size = 1024;
  char cmd[buf_size];
  while (!context->flag_quit_) {
    // read from buffer
    memset(cmd, 0, buf_size);
    int i = 0;
    char ch;
    bool quit_flag = false;
    while ((ch = fgetc(fp)) != ';') {
      if(ch == EOF) { quit_flag = true; break; }
      cmd[i++] = ch;
    }
    if(quit_flag || fgetc(fp) == EOF) break;
    cmd[i] = ch;    // ;
    ///getchar();        // remove enter

    // create buffer for sql input
    YY_BUFFER_STATE bp = yy_scan_string(cmd);
    if (bp == nullptr) {
      LOG(ERROR) << "Failed to create yy buffer state." << std::endl;
      exit(1);
    }
    yy_switch_to_buffer(bp);

    // init parser module
    MinisqlParserInit();

    // parse
    yyparse();

    // parse result handle
    if (MinisqlParserGetError()) {
      // error
      printf("%s\n", MinisqlParserGetErrorMessage());
    }
    ExecuteContext temp_context;
    temp_context.dont_print = true;
    Execute(MinisqlGetParserRootNode(), &temp_context);
    context->rows_num += temp_context.rows_num;
    // clean memory after parse
    MinisqlParserFinish();
    yy_delete_buffer(bp);
    yylex_destroy();
    // quit condition
  }
  PrintInfo(context);
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  ASSERT(ast->type_ == kNodeQuit, "Unexpected node type.");
  context->flag_quit_ = true;
  return DB_SUCCESS;
}
