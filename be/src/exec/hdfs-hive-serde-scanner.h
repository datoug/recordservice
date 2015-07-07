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

#ifndef IMPALA_EXEC_HDFS_HIVE_SERDE_SCANNER_H
#define IMPALA_EXEC_HDFS_HIVE_SERDE_SCANNER_H

#include <jni.h>

#include "exec/hdfs-text-scanner.h"

namespace impala {

class DelimitedTextParser;
class DataSourceRowConverter;
struct HdfsFileDesc;

// HdfsScanner implementation that calls Hive serde classes to parse
// the input (currently only text-formatted) records. This is done by
// calling a Java-side class `HiveSerDeExecutor` through JNI.
// This class uses many of the existing logics from HdfsTextScanner, including
// issuing initial range, split processing, buffer filling, column/row boundary
// handling, etc. The main difference is in WriteFields. Unlike HdfsTextScanner,
// this class wraps the raw buffer, send it to the FE, and calls the
// DataSourceRowConverter to materialize the rows in the results.
class HdfsHiveSerdeScanner : public HdfsTextScanner {
 public:
  HdfsHiveSerdeScanner(HdfsScanNode* scan_node, RuntimeState* state);

  virtual ~HdfsHiveSerdeScanner();
  virtual Status Prepare(ScannerContext* context);
  virtual void Close();

  static Status IssueInitialRanges(HdfsScanNode*, const std::vector<HdfsFileDesc*>&);

 private:
  // Class name for the Java-side executor class
  static const char* EXECUTOR_CLASS;

  // The signature for the constructor of the executor class
  static const char* EXECUTOR_CTOR_SIG;

  // The signature for the deserialize method of the executor class
  static const char* EXECUTOR_DESERIALIZE_SIG;

  // The name for the deserialize method of the executor class
  static const char* EXECUTOR_DESERIALIZE_NAME;

  // Initialize this scanner for a new scan range.
  virtual Status InitNewRange();

  // This overrides HdfsTextScanner::WriteFields. The difference here is
  // that we send buffers to the FE, instead of processing them directly here.
  virtual int WriteFields(MemPool* pool, TupleRow* tuple_row,
                          int num_fields, int num_tuples);

  // Calls the FE HiveSerDeExecutor to process tuples collected so far.
  // `input` contains the buffer to send. The results are materialized in the
  // `tuple_row`, and `num_tuples_materialized` is incremented by the number of tuples
  // materialized in this call.
  Status WriteRowBatch(MemPool* pool, const TSerDeInput& input,
                       TupleRow** tuple_row, int* num_tuples_materialized);

  // Return the specific HDFS file format associated with this scanner.
  virtual inline THdfsFileFormat::type GetTHdfsFileFormat() const {
    return THdfsFileFormat::HIVE_SERDE;
  }

  // An instance of HiveSerDeExecutor object
  jobject executor_;

  // The ref of the HiveSerDeExecutor class
  jclass executor_class_;

  // The constructor id for the executor class
  jmethodID executor_ctor_id_;

  // The deserialize method id for the executor class
  jmethodID executor_deser_id_;

  // Helper class for parsing rows from external data source
  boost::scoped_ptr<DataSourceRowConverter> row_converter_;
};
}

#endif
