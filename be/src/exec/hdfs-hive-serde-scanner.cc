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

#include "exec/hdfs-hive-serde-scanner.h"

#include <jni.h>
#include <string>

#include "exec/delimited-text-parser.h"
#include "exec/delimited-text-parser.inline.h"
#include "exec/external-data-source-executor.h"
#include "exec/hdfs-scan-node.h"
#include "exec/scanner-context.inline.h"
#include "exec/text-converter.inline.h"
#include "rpc/jni-thrift-util.h"
#include "runtime/runtime-state.h"
#include "runtime/tuple.h"
#include "runtime/tuple-row.h"
#include "util/codec.h"
#include "util/jni-util.h"

#include "gen-cpp/ExternalDataSource_types.h"

using namespace boost;
using namespace impala;
using namespace llvm;
using namespace std;

const char* HdfsHiveSerdeScanner::EXECUTOR_CLASS =
    "com/cloudera/impala/hive/serde/HiveSerDeExecutor";
const char* HdfsHiveSerdeScanner::EXECUTOR_CTOR_SIG = "()V";
const char* HdfsHiveSerdeScanner::EXECUTOR_DESERIALIZE_SIG = "([B)[B";
const char* HdfsHiveSerdeScanner::EXECUTOR_DESERIALIZE_NAME = "deserialize";

Status HdfsHiveSerdeScanner::IssueInitialRanges(
    HdfsScanNode* scan_node, const vector<HdfsFileDesc*>& files) {

  for (int i = 0; i < files.size(); ++i) {
    // We're just assuming the files are not compressed
    THdfsCompression::type compression = files[i]->file_compression;
    switch (compression) {
    case THdfsCompression::NONE:
      RETURN_IF_ERROR(scan_node->AddDiskIoRanges(files[i]));
      break;
    default:
      DCHECK(false) << "Cannot handle compressed file format yet.";
    }
  }

  return Status::OK;
}

Status HdfsHiveSerdeScanner::InitNewRange() {
  HdfsPartitionDescriptor* hdfs_partition = context_->partition_descriptor();
  // We're passing it by pointer, and the value for variable will be
  // hold on stack (and perhaps overwritten) if it's non-static.
  static bool is_materialized_col = true;

  delimited_text_parser_.reset(new DelimitedTextParser(
      1, 0, &is_materialized_col, hdfs_partition->line_delim()));

  return Status::OK;
}

HdfsHiveSerdeScanner::HdfsHiveSerdeScanner(HdfsScanNode* scan_node, RuntimeState* state)
  : HdfsScanner(scan_node, state), byte_buffer_ptr_(NULL),
    byte_buffer_end_(NULL), byte_buffer_read_size_(0),
    executor_(NULL), executor_class_(NULL), executor_ctor_id_(NULL),
    executor_deser_id_(NULL) {
}

HdfsHiveSerdeScanner::~HdfsHiveSerdeScanner() {
}

Status HdfsHiveSerdeScanner::Prepare(ScannerContext* context) {
  RETURN_IF_ERROR(HdfsScanner::Prepare(context));

  field_locations_.resize(state_->batch_size());
  row_end_locations_.resize(state_->batch_size());

  JNIEnv* env = getJNIEnv();
  JniLocalFrame jni_frame;
  RETURN_IF_ERROR(jni_frame.push(env));

  // Find out constructor and deserialize method id of the executor class.
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env, EXECUTOR_CLASS, &executor_class_));
  executor_ctor_id_ = env->GetMethodID(executor_class_, "<init>", EXECUTOR_CTOR_SIG);
  RETURN_ERROR_IF_EXC(env);
  executor_deser_id_ = env->GetMethodID(
      executor_class_, EXECUTOR_DESERIALIZE_NAME, EXECUTOR_DESERIALIZE_SIG);
  RETURN_ERROR_IF_EXC(env);

  // Create the java executor object
  executor_ = env->NewObject(executor_class_, executor_ctor_id_);
  RETURN_ERROR_IF_EXC(env);
  executor_ = env->NewGlobalRef(executor_);
  RETURN_ERROR_IF_EXC(env);

  return Status::OK;
}

Status HdfsHiveSerdeScanner::ProcessSplit() {
  // Reset state for the new scan range
  RETURN_IF_ERROR(InitNewRange());
  RETURN_IF_ERROR(ProcessRange());
  return Status::OK;
}

Status HdfsHiveSerdeScanner::ProcessRange() {
  bool eosr = stream_->eosr();

  while (true) {
    if (!eosr && byte_buffer_ptr_ == byte_buffer_end_) {
      RETURN_IF_ERROR(FillByteBuffer(&eosr));
    }

    // First, use delimited text parser to find row boundaries
    // from the buffer.
    MemPool* pool;
    TupleRow* tuple_row;
    int max_tuples = GetMemory(&pool, &tuple_, &tuple_row);
    DCHECK_GT(max_tuples, 0);

    int num_tuples = 0;
    int num_fields = 0;
    int num_commit = 0;
    char* col_start;
    char* buffer_start = byte_buffer_ptr_;

    RETURN_IF_ERROR(delimited_text_parser_->ParseFieldLocations(
        max_tuples, byte_buffer_read_size_, &byte_buffer_ptr_,
        &row_end_locations_[0], &field_locations_[0], &num_tuples,
        &num_fields, &col_start));

    // Construct a TSerDeInput which consists of the byte buffer and
    // a list of lengths for each row in the buffer.
    TSerDeInput input;

    // Find out each row's length. We also need to take count
    // of the row delimiter.
    for (int i = 0; i < num_tuples; ++i) {
      int len = row_end_locations_[i] -
          (i == 0 ? buffer_start : (row_end_locations_[i-1] + 1));
      input.lengths.push_back(len);
    }

    // Pass a string (ByteBuffer on the Java side) through thrift.
    // TODO: optimize this further
    input.data = string(buffer_start,
        row_end_locations_[num_tuples - 1] - buffer_start + 1);

    // Call the FE side Java serde executor
    JNIEnv* env = getJNIEnv();
    JniLocalFrame jni_frame;
    RETURN_IF_ERROR(jni_frame.push(env));

    jbyteArray input_bytes;
    jbyteArray output_bytes;

    RETURN_IF_ERROR(SerializeThriftMsg(env, &input, &input_bytes));
    output_bytes = (jbyteArray)
        env->CallObjectMethod(executor_, executor_deser_id_, input_bytes);

    // The output from the executor call is a RowBatch.
    TSerDeOutput output;
    RETURN_IF_ERROR(DeserializeThriftMsg(env, output_bytes, &output));
    const vector<TColumnData>& cols = output.batch.cols;
    // TODO: remove this check once the serde class is working
    DCHECK_EQ(cols.size(), 1);

    for (int i = 0; i < output.batch.num_rows; ++i) {
      MaterializeRecord(tuple_, pool, cols[0].string_vals[i]);
      tuple_row->SetTuple(scan_node_->tuple_idx(), tuple_);
      if (EvalConjuncts(tuple_row)) {
        ++num_commit;
        tuple_ = next_tuple(tuple_);
        tuple_row = next_row(tuple_row);
      }
    }

    COUNTER_ADD(scan_node_->rows_read_counter(), num_tuples);

    // Commit the rows to the row batch and scan node
    RETURN_IF_ERROR(CommitRows(num_commit));
    if ((byte_buffer_ptr_ == byte_buffer_end_) && eosr) break;
    if (scan_node_->ReachedLimit()) break;
  }

  return Status::OK;
}

void HdfsHiveSerdeScanner::MaterializeRecord(Tuple* tuple, MemPool* pool,
                                               const string& row) {
  InitTuple(template_tuple_, tuple);

  // TODO: better to move this logic and the for loop above into
  // data-source-scan-node
  for (int i = 0; i < scan_node_->materialized_slots().size(); ++i) {
    SlotDescriptor* desc = scan_node_->materialized_slots()[i];
    if (i == 0) {
      StringValue* slot = reinterpret_cast<StringValue*>(
          tuple->GetSlot(desc->tuple_offset()));
      slot->len = row.length();
      slot->ptr = reinterpret_cast<char*>(pool->Allocate(slot->len + 1));
      memcpy(slot->ptr, row.c_str(), slot->len + 1);
    } else {
      // TODO: fill the rest of the slots once hive serde is working
      tuple->SetNull(desc->null_indicator_offset());
    }
  }
}

Status HdfsHiveSerdeScanner::FillByteBuffer(bool* eosr) {
  RETURN_IF_ERROR(stream_->GetBuffer(false,
      reinterpret_cast<uint8_t**>(&byte_buffer_ptr_),
      &byte_buffer_read_size_));

  *eosr = stream_->eosr();
  byte_buffer_end_ = byte_buffer_ptr_ + byte_buffer_read_size_;

  return Status::OK;
}

void HdfsHiveSerdeScanner::Close() {
  AddFinalRowBatch();
  scan_node_->RangeComplete(THdfsFileFormat::TEXT, THdfsCompression::NONE);

  // clean up JNI stuff
  if (executor_ != NULL) {
    JNIEnv* env = getJNIEnv();
    env->DeleteGlobalRef(executor_);

    Status status = JniUtil::GetJniExceptionMsg(env, "HdfsHiveSerdeScanner::Close(): ");
    if (!status.ok()) state_->LogError(status.msg());
  }

  HdfsScanner::Close();
}
