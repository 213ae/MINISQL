#include <cstdio>
#include "executor/execute_engine.h"
#include "glog/logging.h"
#include "parser/syntax_tree_printer.h"
#include "utils/tree_file_mgr.h"

extern "C" {
int yyparse(void);
FILE *yyin;
#include "parser/minisql_lex.h"
#include "parser/parser.h"
}
void InitGoogleLog(char *argv) {
  FLAGS_logtostderr = true;
  FLAGS_colorlogtostderr = true;
  google::InitGoogleLogging(argv);
}

void InputCommand(char *input, const int len) {
  memset(input, 0, len);
  printf("minisql > ");
  int i = 0;
  char ch;
  while ((ch = getchar()) != ';') {
    input[i++] = ch;
  }
  input[i] = ch;    // ;
  getchar();        // remove enter
}

int main(int argc, char **argv) {
  cout << "Loading..." << endl;
  InitGoogleLog(argv[0]);
  // command buffer
  const int buf_size = 1024;
  char cmd[buf_size];
  // execute engine
  struct timeval start{}, end{};
  gettimeofday(&start, nullptr);
  ExecuteEngine engine;
  gettimeofday(&end, nullptr);
  cout << "Done(" <<  end.tv_sec - start.tv_sec << " sec)" << endl;
  // for print syntax tree
  TreeFileManagers syntax_tree_file_mgr("syntax_tree_");
  [[maybe_unused]] uint32_t syntax_tree_id = 0;
  int i = 1;
  while (i++) {
    // read from buffer
    InputCommand(cmd, buf_size);
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
    } else {
#ifdef ENABLE_PARSER_DEBUG
      printf("[INFO] Sql syntax parse ok!\n");
      SyntaxTreePrinter printer(MinisqlGetParserRootNode());
      printer.PrintTree(syntax_tree_file_mgr[syntax_tree_id++]);
#endif
    }

    ExecuteContext context;
    SyntaxTreePrinter printer(MinisqlGetParserRootNode());

    //if(i < 20) printer.PrintTree(syntax_tree_file_mgr[i]);
    engine.Execute(MinisqlGetParserRootNode(), &context);

    //sleep(1);

    // clean memory after parse
    MinisqlParserFinish();
    yy_delete_buffer(bp);
    yylex_destroy();

    // quit condition
    if (context.flag_quit_) {
      printf("bye!\n");
      break;
    }

  }
  return 0;
}