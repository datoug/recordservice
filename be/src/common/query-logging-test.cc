// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <string>
#include <gtest/gtest.h>
#include "common/query-logging.h"
#include "common/names.h"

namespace impala {

TEST(QueryLoggingTest, Basic) {
  Logger logger("test: ", 2);
  QUERY_VLOG(&logger, 1) << "vlog 1";
  QUERY_VLOG(&logger, 2) << "vlog 2";
  QUERY_VLOG(&logger, 3) << "vlog 3"; // Should not show up.

  QUERY_VLOG_WARNING(&logger) << "warning";
  QUERY_VLOG_FRAGMENT(&logger) << "fragment";
  QUERY_VLOG_FILE(&logger) << "file";
  QUERY_VLOG_BATCH(&logger) << "batch";
  QUERY_VLOG_ROW(&logger) << "row";
}

}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
