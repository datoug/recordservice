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

#include "exec/hdfs-scanner.h"

namespace impala {

class DelimitedTextParser;
struct HdfsFileDesc;

// HdfsScanner implementation that calls Hive serde classes to parse
// the input (currently only text-formatted) records. This is done by
// calling a Java-side class `HiveSerDeExecutor` through JNI.

// TODO: implement more sophisticated text parsing, e.g., tuple/field
// boundary handling, timer, etc.
class HdfsHiveSerdeScanner : public HdfsScanner {
 public:
  HdfsHiveSerdeScanner(HdfsScanNode* scan_node, RuntimeState* state);

  virtual ~HdfsHiveSerdeScanner();
  virtual Status Prepare(ScannerContext* context);
  virtual Status ProcessSplit();
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

  // Process the entire scan range, reading bytes from context and appending
  // materialized row batches to the scan node.
  Status ProcessRange();

  // Fill the next byte buffer from context. This will block if there are
  // no more bytes ready.
  virtual Status FillByteBuffer(bool* eosr);

  // Write a single string row returned from the JNI call into the input tuple
  void MaterializeRecord(Tuple* tuple, MemPool* pool, const string& row);

  // Current position in the buffer returned from ScannerContext
  char* byte_buffer_ptr_;

  // End position of the current buffer returned from ScannerContext
  char* byte_buffer_end_;

  // Actual bytes received from last file read
  int64_t byte_buffer_read_size_;

  // An instance of HiveSerDeExecutor object
  jobject executor_;

  // The ref of the HiveSerDeExecutor class
  jclass executor_class_;

  // The constructor id for the executor class
  jmethodID executor_ctor_id_;

  // The deserialize method id for the executor class
  jmethodID executor_deser_id_;

  // Helper class for picking rows from delimited text
  boost::scoped_ptr<DelimitedTextParser> delimited_text_parser_;

  // Return field locations from the delimited text parser.
  // Not needed for this class, just to keep the parser happy.
  std::vector<FieldLocation> field_locations_;

  // Pointers into scanner context's byte buffer for the end ptr
  // locations of each row processed in the current batch.
  std::vector<char*> row_end_locations_;
};
}

#endif
