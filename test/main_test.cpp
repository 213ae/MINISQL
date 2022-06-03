#include "gtest/gtest.h"
#include "glog/logging.h"

int main(int argc, char **argv) {
  sleep(5);
  testing::InitGoogleTest(&argc, argv);
  // testing::GTEST_FLAG(filter) = "BPlusTreeTests*";
  FLAGS_logtostderr = true;
  FLAGS_colorlogtostderr = true;
  google::InitGoogleLogging(argv[0]);
  return RUN_ALL_TESTS();
}