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

#include "exec/record-service-scan-node.h"

#include <exception>
#include <sstream>
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <gutil/strings/substitute.h>

#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/tuple.h"
#include "runtime/tuple-row.h"

using namespace boost;
using namespace impala;
using namespace llvm;
using namespace std;
using namespace strings;

DECLARE_int32(recordservice_worker_port);

RecordServiceScanNode::RecordServiceScanNode(
    ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
  : ScanNode(pool, tnode, descs),
    tuple_id_(tnode.hdfs_scan_node.tuple_id) {
}

RecordServiceScanNode::~RecordServiceScanNode() {
}

Status RecordServiceScanNode::Prepare(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ScanNode::Prepare(state));

  tuple_desc_ = state->desc_tbl().GetTupleDescriptor(tuple_id_);
  DCHECK(tuple_desc_ != NULL);
  tuple_byte_size_ = tuple_desc_->byte_size();

  DCHECK(tuple_desc_->table_desc() != NULL);
  hdfs_table_ = static_cast<const HdfsTableDescriptor*>(tuple_desc_->table_desc());
  const vector<SlotDescriptor*>& slots = tuple_desc_->slots();
  stringstream stmt;
  // TODO: handle count(*)
  for (size_t i = 0; i < slots.size(); ++i) {
    if (!slots[i]->is_materialized()) continue;
    int col_idx = slots[i]->col_pos();
    materialized_slots_.push_back(slots[i]);
    materialized_col_names_.push_back(hdfs_table_->col_names()[col_idx]);
    if (materialized_col_names_.size() == 1) {
      stmt << materialized_col_names_[i];
    } else {
      stmt << ", " << materialized_col_names_[i];
    }
  }
  stmt << " FROM " << hdfs_table_->database() << "." << hdfs_table_->name();

  DCHECK(scan_range_params_ != NULL)
      << "Must call SetScanRanges() before calling Prepare()";
  tasks_.resize(scan_range_params_->size());
  for (int i = 0; i < scan_range_params_->size(); ++i) {
    DCHECK((*scan_range_params_)[i].scan_range.__isset.hdfs_file_split);
    const THdfsFileSplit& split = (*scan_range_params_)[i].scan_range.hdfs_file_split;
    HdfsPartitionDescriptor* partition_desc =
        hdfs_table_->GetPartition(split.partition_id);
    filesystem::path file_path(partition_desc->location());
    file_path.append(split.file_name, filesystem::path::codecvt());
    const string& native_file_path = file_path.native();

    stringstream ss;
    ss << "SELECT /* +__input_split__=" << native_file_path << "@"
       << split.offset << "@" << split.length << " */ " << stmt.str();
    tasks_[i].stmt = ss.str();
  }
  task_id_ = 0;

  return Status::OK;
}

Status RecordServiceScanNode::Open(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  // Connect to the local record service worker
  rsw_client_.reset(
      new ThriftClient<recordservice::RecordServiceWorkerClient>("localhost",
          FLAGS_recordservice_worker_port));
  RETURN_IF_ERROR(rsw_client_->Open());
  return Status::OK;
}

// TODO: Parallelize this at the task level.
Status RecordServiceScanNode::GetNext(RuntimeState* state,
    RowBatch* row_batch, bool* eos) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  if (task_id_ >= tasks_.size() || ReachedLimit()) {
    *eos = true;
    return Status::OK;
  }
  *eos = false;

  if (!tasks_[task_id_].connected) {
    // Starting a new task
    recordservice::TExecTaskParams params;
    params.task = tasks_[task_id_].stmt;
    params.__set_row_batch_format(recordservice::TRowBatchFormat::Parquet);
    recordservice::TExecTaskResult result;

    try {
      rsw_client_->iface()->ExecTask(result, params);
    } catch (const recordservice::RecordServiceException& e) {
      return Status(e.message.c_str());
    } catch (const std::exception& e) {
      return Status(e.what());
    }

    tasks_[task_id_].handle = result.handle;
    tasks_[task_id_].connected = true;
  }

  // Fetch the next batch from the current task.
  recordservice::TFetchResult fetch_result;
  recordservice::TFetchParams fetch_params;
  fetch_params.handle = tasks_[task_id_].handle;
  try {
    rsw_client_->iface()->Fetch(fetch_result, fetch_params);
    if (!fetch_result.__isset.parquet_row_batch) {
      return Status("Expecting record service to return parquet row batches.");
    }

    // Convert into row_batch here.
    const recordservice::TParquetRowBatch& input_batch = fetch_result.parquet_row_batch;

    // TODO: validate schema.
    if (input_batch.cols.size() != materialized_slots_.size()) {
      stringstream ss;
      ss << "Invalid row batch from record service. Expecting "
         << materialized_slots_.size()
         << " cols. Record service returned " << input_batch.cols.size() << " cols.";
      return Status(ss.str());
    }

    Tuple* tuple = Tuple::Create(row_batch->MaxTupleBufferSize(),
        row_batch->tuple_data_pool());

    // TODO: this really needs codegen/optimizations
    vector<const char*> data_values;
    for (int i = 0; i < input_batch.cols.size(); ++i) {
      data_values.push_back(input_batch.cols[i].data.data());
    }

    for (int i = 0; i < fetch_result.num_rows; ++i) {
      TupleRow* row = row_batch->GetRow(row_batch->AddRow());
      row->SetTuple(0, tuple);

      for (int c = 0; c < materialized_slots_.size(); ++c) {
        const recordservice::TParquetColumnData& data = input_batch.cols[c];
        if (data.is_null[i]) {
          tuple->SetNull(materialized_slots_[c]->null_indicator_offset());
          continue;
        }

        tuple->SetNotNull(materialized_slots_[c]->null_indicator_offset());
        void* slot = tuple->GetSlot(materialized_slots_[c]->tuple_offset());
        switch (materialized_slots_[c]->type().type) {
          case TYPE_BOOLEAN:
          case TYPE_TINYINT:
          case TYPE_SMALLINT:
          case TYPE_INT:
          case TYPE_BIGINT:
          case TYPE_FLOAT:
          case TYPE_DOUBLE:
          case TYPE_TIMESTAMP:
          case TYPE_DECIMAL:
            memcpy(slot, data_values[c], materialized_slots_[c]->type().GetByteSize());
            data_values[c] += materialized_slots_[c]->type().GetByteSize();
            break;

          case TYPE_STRING:
          case TYPE_VARCHAR: {
            // TODO: this copy can be removed by having the row batch take ownership
            // of the string data from the TParquetRowBatch.
            StringValue* sv = reinterpret_cast<StringValue*>(slot);
            sv->len = *reinterpret_cast<const int32_t*>(data_values[c]);
            data_values[c] += sizeof(int32_t);
            sv->ptr = reinterpret_cast<char*>(
                row_batch->tuple_data_pool()->Allocate(sv->len));
            memcpy(sv->ptr, data_values[c], sv->len);
            data_values[c] += sv->len;
            break;
          }

          default:
            CHECK(false) << "Not implemented";
        }
      }

      if (EvalConjuncts(&conjunct_ctxs_[0], conjunct_ctxs_.size(), row)) {
        row_batch->CommitLastRow();
        tuple = next_tuple(tuple);
        ++num_rows_returned_;
        COUNTER_SET(rows_returned_counter_, num_rows_returned_);
        if (ReachedLimit()) break;
      }
    }

    if (fetch_result.done) {
      // Move onto next task
      rsw_client_->iface()->CloseTask(tasks_[task_id_].handle);
      ++task_id_;
    }
  } catch (const recordservice::RecordServiceException& e) {
    return Status(e.message.c_str());
  } catch (const std::exception& e) {
    return Status(e.what());
  }

  return Status::OK;
}

void RecordServiceScanNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  for (int i = 0; i < tasks_.size(); ++i) {
    if (tasks_[i].connected) {
      try {
        rsw_client_->iface()->CloseTask(tasks_[i].handle);
        tasks_[i].connected = false;
      } catch (const recordservice::RecordServiceException& e) {
        state->LogError(e.message);
      } catch (const std::exception& e) {
        state->LogError(e.what());
      }
    }
  }
  ScanNode::Close(state);
}