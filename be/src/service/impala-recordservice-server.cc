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

#include "service/impala-server.h"
#include "service/impala-server.inline.h"

#include <algorithm>
#include <boost/algorithm/string/join.hpp>
#include <boost/date_time/posix_time/posix_time_types.hpp>
#include <boost/unordered_set.hpp>
#include <thrift/protocol/TDebugProtocol.h>
#include <boost/foreach.hpp>
#include <boost/algorithm/string.hpp>
#include <gutil/strings/substitute.h>

#include "common/logging.h"
#include "common/version.h"
#include "exprs/expr.h"
#include "exprs/expr-context.h"
#include "runtime/raw-value.h"
#include "service/query-exec-state.h"
#include "service/query-options.h"
#include "util/debug-util.h"
#include "rpc/thrift-util.h"
#include "util/impalad-metrics.h"
#include "service/hs2-util.h"

using namespace std;
using namespace boost;
using namespace strings;
using namespace beeswax; // Converting QueryState

static const int DEFAULT_FETCH_SIZE = 5000;

namespace impala {

inline void ThrowException(const string& msg) {
  recordservice::RecordServiceException ex;
  ex.message = msg;
  throw ex;
}

// Base class for test result set serializations. The functions in here and
// not used in the record service path.
//
// Used to abstract away serializing results. The calling pattern is:
//
// BaseResult* result = new ...
// result->Init();
// for each rpc:
//   result->SetReturnBuffer();
//   for each batch:
//     result->AddBatch()
//   result->FinalizeResult()
class ImpalaServer::BaseResultSet : public ImpalaServer::QueryResultSet {
 public:
  // Convert TResultRow to ASCII using "\t" as column delimiter and store it in this
  // result set.
  virtual Status AddOneRow(const TResultRow& row) {
    CHECK(false) << "Not used";
    return Status::OK;
  }

  virtual int AddRows(const QueryResultSet* other, int start_idx, int num_rows) {
    CHECK(false) << "Not used";
    return num_rows;
  }

  virtual int64_t ByteSize(int start_idx, int num_rows) {
    CHECK(false) << "Not used";
    return sizeof(int64_t);
  }

  virtual Status AddOneRow(const vector<void*>& col_values, const vector<int>& scales) {
    CHECK(false) << "Not used";
    return Status::OK;
  }

  virtual void Init(const TResultSetMetadata& md, int fetch_size) {
    for (int i = 0; i < md.columns.size(); ++i) {
      types_.push_back(md.columns[i].columnType);
      type_sizes_.push_back(types_[i].GetByteSize());
    }
  }

  virtual void FinalizeResult() {}

  virtual bool supports_batch_add() const { return true; }

  // This should be set for every fetch request so that the results are directly
  // populated in the thrift result object (to avoid a copy).
  virtual void SetReturnBuffer(recordservice::TFetchResult* result) = 0;

  virtual size_t size() { return result_ == NULL ? 0 :result_->num_rows; }

 protected:
  BaseResultSet() : result_(NULL) {}
  recordservice::TFetchResult* result_;

  vector<ColumnType> types_;
  vector<int> type_sizes_;
};

// Additional state for the record service. Put here instead of QueryExecState
// to separate from Impala code.
class ImpalaServer::RecordServiceTaskState {
 public:
  int fetch_size;
  recordservice::TRowBatchFormat::type format;
  scoped_ptr<ImpalaServer::BaseResultSet> results;
};

class ImpalaServer::RecordServiceColumnarResultSet : public ImpalaServer::BaseResultSet {
 public:
  RecordServiceColumnarResultSet() {}

  virtual void SetReturnBuffer(recordservice::TFetchResult* result) {
    result_ = result;
    result_->__isset.row_batch = true;

    recordservice::TColumnarRowBatch& batch = result_->row_batch;
    batch.cols.resize(types_.size());

    for (int i = 0; i < types_.size(); ++i) {
      switch (types_[i].type) {
        case TYPE_BOOLEAN:
          batch.cols[i].__isset.bool_vals = true;
          break;
        case TYPE_TINYINT:
          batch.cols[i].__isset.byte_vals = true;
          break;
        case TYPE_SMALLINT:
          batch.cols[i].__isset.short_vals = true;
          break;
        case TYPE_INT:
          batch.cols[i].__isset.int_vals = true;
          break;
        case TYPE_BIGINT:
          batch.cols[i].__isset.long_vals = true;
          break;
        case TYPE_FLOAT:
        case TYPE_DOUBLE:
          batch.cols[i].__isset.double_vals = true;
          break;
        case TYPE_VARCHAR:
        case TYPE_STRING:
          batch.cols[i].__isset.string_vals = true;
          break;
        case TYPE_TIMESTAMP:
        case TYPE_DECIMAL:
          batch.cols[i].__isset.binary_vals = true;
          break;
        default:
          CHECK(false) << "not implemented";
      }
    }
  }

  virtual Status AddOneRow(const vector<void*>& col_values, const vector<int>& scales) {
    DCHECK_EQ(col_values.size(), types_.size());
    for (int i = 0; i < col_values.size(); ++i) {
      AppendValue(i, col_values[i]);
    }
    ++result_->num_rows;
    return Status::OK;
  }

  virtual void AddRowBatch(RowBatch* batch, int row_idx, int num_rows,
      vector<ExprContext*>* ctxs) {
    for (int i = 0; i < num_rows; ++i) {
      TupleRow* row = batch->GetRow(row_idx++);
      for (int c = 0; c < ctxs->size(); ++c) {
        AppendValue(c, (*ctxs)[c]->GetValue(row));
      }
    }
    result_->num_rows += num_rows;
  }

  inline void AppendValue(int col_idx, const void* v) {
    DCHECK(result_->__isset.row_batch);
    recordservice::TColumnarRowBatch& batch = result_->row_batch;

    batch.cols[col_idx].is_null.push_back(v == NULL);
    if (v == NULL) return;

    switch (types_[col_idx].type) {
      case TYPE_BOOLEAN:
        batch.cols[col_idx].bool_vals.push_back(*reinterpret_cast<const bool*>(v));
        break;
      case TYPE_TINYINT:
        batch.cols[col_idx].byte_vals.push_back(*reinterpret_cast<const uint8_t*>(v));
        break;
      case TYPE_SMALLINT:
        batch.cols[col_idx].short_vals.push_back(*reinterpret_cast<const int16_t*>(v));
        break;
      case TYPE_INT:
        batch.cols[col_idx].int_vals.push_back(*reinterpret_cast<const int32_t*>(v));
        break;
      case TYPE_BIGINT:
        batch.cols[col_idx].long_vals.push_back(*reinterpret_cast<const int64_t*>(v));
        break;
      case TYPE_FLOAT:
        batch.cols[col_idx].double_vals.push_back(*reinterpret_cast<const float*>(v));
        break;
      case TYPE_DOUBLE:
        batch.cols[col_idx].double_vals.push_back(*reinterpret_cast<const double*>(v));
        break;
      case TYPE_VARCHAR:
      case TYPE_STRING: {
        const StringValue* sv = reinterpret_cast<const StringValue*>(v);
        batch.cols[col_idx].string_vals.push_back(sv->DebugString());
        break;
      }
      case TYPE_TIMESTAMP:
        batch.cols[col_idx].binary_vals.push_back(string((const char*)v, 16));
        break;
      case TYPE_DECIMAL:
        batch.cols[col_idx].binary_vals.push_back(
            string((const char*)v, types_[col_idx].GetByteSize()));
        break;
      default:
        CHECK(false) << "not implemented";
    }
  }
};

// This is the parquet plain encoding, meaning we append the little endian version
// of the value to the end of the buffer.
class ImpalaServer::RecordServiceParquetResultSet : public ImpalaServer::BaseResultSet {
 public:
  virtual void SetReturnBuffer(recordservice::TFetchResult* result) {
    result_ = result;
    result_->__isset.parquet_row_batch = true;

    recordservice::TParquetRowBatch& batch = result_->parquet_row_batch;
    batch.cols.resize(types_.size());
  }

  virtual void AddRowBatch(RowBatch* input, int row_idx, int num_rows,
      vector<ExprContext*>* ctxs) {
    DCHECK(result_->__isset.parquet_row_batch);
    recordservice::TParquetRowBatch& batch = result_->parquet_row_batch;

    // Reserve the size of the output where possible.
    for (int c = 0; c < ctxs->size(); ++c) {
      batch.cols[c].is_null.reserve(batch.cols[c].data.size() + num_rows);
      if (type_sizes_[c] != 0) {
        batch.cols[c].data.reserve(batch.cols[c].data.size() + num_rows * type_sizes_[c]);
      }
    }

    for (int i = 0; i < num_rows; ++i) {
      TupleRow* row = input->GetRow(row_idx++);

      for (int c = 0; c < ctxs->size(); ++c) {
        const void* v = (*ctxs)[c]->GetValue(row);
        batch.cols[c].is_null.push_back(v == NULL);
        if (v == NULL) continue;

        string& data = batch.cols[c].data;
        int offset = data.size();

        // Encode the values here. For non-string types, just write the value as
        // little endian. For strings, it is the length(little endian) followed
        // by the string.
        const int type_size = type_sizes_[c];
        if (type_size == 0) {
          const StringValue* sv = reinterpret_cast<const StringValue*>(v);
          int len = sv->len + sizeof(int32_t);
          data.resize(offset + len);
          memcpy((char*)data.data() + offset, &sv->len, sizeof(int32_t));
          memcpy((char*)data.data() + offset + sizeof(int32_t), sv->ptr, sv->len);
        } else {
          data.resize(offset + type_size);
          memcpy((char*)data.data() + offset, v, type_size);
        }
      }
    }
    result_->num_rows += num_rows;
  }
};

shared_ptr<ImpalaServer::SessionState> ImpalaServer::GetRecordServiceSession() {
  unique_lock<mutex> l(connection_to_sessions_map_lock_);
  if (record_service_session_.get() == NULL) {
    record_service_session_.reset(new SessionState());
    record_service_session_->session_type = TSessionType::RECORDSERVICE;
    record_service_session_->start_time = TimestampValue::local_time();
    record_service_session_->last_accessed_ms = ms_since_epoch();
    record_service_session_->database = "default";
    record_service_session_->ref_count = 1;
  }
  return record_service_session_;
}

recordservice::TType ToRecordServiceType(const ColumnType& t) {
  recordservice::TType result;
  switch (t.type) {
    case TYPE_BOOLEAN:
      result.type_id = recordservice::TTypeId::BOOLEAN;
      break;
    case TYPE_TINYINT:
      result.type_id = recordservice::TTypeId::TINYINT;
      break;
    case TYPE_SMALLINT:
      result.type_id = recordservice::TTypeId::SMALLINT;
      break;
    case TYPE_INT:
      result.type_id = recordservice::TTypeId::INT;
      break;
    case TYPE_BIGINT:
      result.type_id = recordservice::TTypeId::BIGINT;
      break;
    case TYPE_FLOAT:
      result.type_id = recordservice::TTypeId::FLOAT;
      break;
    case TYPE_DOUBLE:
      result.type_id = recordservice::TTypeId::DOUBLE;
      break;
    case TYPE_STRING:
      result.type_id = recordservice::TTypeId::STRING;
      break;
    case TYPE_TIMESTAMP:
      result.type_id = recordservice::TTypeId::TIMESTAMP;
      break;
    case TYPE_DECIMAL:
      result.type_id = recordservice::TTypeId::DECIMAL;
      result.__set_precision(t.precision);
      result.__set_scale(t.scale);
      break;
    default:
      ThrowException("Not supported type.");
  }
    return result;
}

//
// RecordServicePlanner
//
void ImpalaServer::PlanRequest(recordservice::TPlanRequestResult& return_val,
  const recordservice::TPlanRequestParams& req) {
  LOG(ERROR) << "RecordService::PlanRequest: " << req.sql_stmt;

  if (IsOffline()) {
    ThrowException("This Impala server is offline. Please retry your query later.");
  }

  TQueryCtx query_ctx;
  query_ctx.request.stmt = req.sql_stmt;

  // Setting num_nodes = 1 means we generate a single node plan which has
  // a simpler structure. It also prevents Impala from analyzing multi-table
  // queries, i.e. joins.
  query_ctx.request.query_options.__set_num_nodes(1);

  // Get the plan. This is the normal Impala plan.
  TExecRequest result;
  Status status = exec_env_->frontend()->GetExecRequest(query_ctx, &result);
  if (!status.ok()) ThrowException(status.GetErrorMsg());

  if (result.stmt_type != TStmtType::QUERY) ThrowException("Cannot run non-SELECT stmts");

  // Extract the types of the result.
  DCHECK(result.__isset.result_set_metadata);
  const TResultSetMetadata& metadata = result.result_set_metadata;
  return_val.schema.cols.resize(metadata.columns.size());
  for (int i = 0; i < metadata.columns.size(); ++i) {
    ColumnType type(metadata.columns[i].columnType);
    return_val.schema.cols[i].type = ToRecordServiceType(type);
    return_val.schema.cols[i].name = metadata.columns[i].columnName;
  }

  // Walk the plan to compute the tasks. We want to find the scan nodes
  // and distribute them.
  // TODO: total hack. We're reverse engineering the planner output here.
  // Update planner.
  const TQueryExecRequest query_request = result.query_exec_request;

  // Reconstruct the file paths from partitions and splits
  DCHECK_EQ(query_request.desc_tbl.tableDescriptors.size(), 1)
      << "single table scans should have 1 table desc set.";
  const map<int64_t, THdfsPartition>& partitions =
      query_request.desc_tbl.tableDescriptors[0].hdfsTable.partitions;

  map<int64_t, string> partition_dirs;
  for (map<int64_t, THdfsPartition>::const_iterator it = partitions.begin();
      it != partitions.end(); ++it) {
    partition_dirs[it->first] = it->second.location;
  }

  DCHECK_EQ(query_request.per_node_scan_ranges.size(), 1)
      << "single node plan should have 1 plan node";
  const vector<TScanRangeLocations>& ranges =
      query_request.per_node_scan_ranges.begin()->second;

  // Rewrite the original sql stmt with the input split hint inserted.
  string sql = req.sql_stmt;
  transform(sql.begin(), sql.end(), sql.begin(), ::tolower);
  sql = sql.substr(sql.find("select") + 6);

  for (int i = 0; i < ranges.size(); ++i) {
    const THdfsFileSplit& split = ranges[i].scan_range.hdfs_file_split;
    if (partition_dirs.find(split.partition_id) == partition_dirs.end()) {
      DCHECK(false) << "Invalid plan request.";
    }

    stringstream ss;
    ss << "SELECT /* +__input_split__="
       << partition_dirs[split.partition_id] << "/" << split.file_name
       << "@" << split.offset << "@" << split.length
       << " */ " << sql;

    recordservice::TTask task;
    task.task = ss.str();
    for (int j = 0; j < ranges[i].locations.size(); ++j) {
      task.hosts.push_back(
          query_request.host_list[ranges[i].locations[j].host_idx].hostname);
    }
    return_val.tasks.push_back(task);
  }
}

//
// RecordServiceWorker
//
void ImpalaServer::ExecTask(recordservice::TExecTaskResult& return_val,
    const recordservice::TExecTaskParams& req) {
  if (IsOffline()) {
    ThrowException("This Impala server is offline. Please retry your query later.");
  }

  GetRecordServiceSession();

  LOG(ERROR) << "RecordService::ExecRequest: " << req.task;
  TQueryCtx query_ctx;
  query_ctx.request.stmt = req.task;
  // These are single scan queries. No need to make distributed plan with extra
  // exchange nodes.
  query_ctx.request.query_options.__set_num_nodes(1);
  if (req.__isset.fetch_size) {
    query_ctx.request.query_options.__set_batch_size(req.fetch_size);
  }

  shared_ptr<QueryExecState> exec_state;
  Status status = Execute(&query_ctx, record_service_session_, &exec_state);
  if (!status.ok()) ThrowException(status.GetErrorMsg());

  shared_ptr<RecordServiceTaskState> task_state(new RecordServiceTaskState());
  exec_state->SetRecordServiceTaskState(task_state);

  task_state->fetch_size = DEFAULT_FETCH_SIZE;
  if (req.__isset.fetch_size) task_state->fetch_size = req.fetch_size;

  task_state->format = recordservice::TRowBatchFormat::ColumnarThrift;
  if (req.__isset.row_batch_format) task_state->format = req.row_batch_format;
  switch (task_state->format) {
    case recordservice::TRowBatchFormat::ColumnarThrift:
      task_state->results.reset(new RecordServiceColumnarResultSet());
      break;
    case recordservice::TRowBatchFormat::Parquet:
      task_state->results.reset(new RecordServiceParquetResultSet());
      break;
    default:
      ThrowException("Service does not support this row batch format.");
  }
  task_state->results->Init(*exec_state->result_metadata(), task_state->fetch_size);

  exec_state->UpdateQueryState(QueryState::RUNNING);
  exec_state->WaitAsync();
  status = SetQueryInflight(record_service_session_, exec_state);
  if (!status.ok()) {
    UnregisterQuery(exec_state->query_id(), false, &status);
  }
  return_val.handle.hi = exec_state->query_id().hi;
  return_val.handle.lo = exec_state->query_id().lo;
}

void ImpalaServer::Fetch(recordservice::TFetchResult& return_val,
    const recordservice::TFetchParams& req) {
  TUniqueId query_id;
  query_id.hi = req.handle.hi;
  query_id.lo = req.handle.lo;

  shared_ptr<QueryExecState> exec_state = GetQueryExecState(query_id, false);
  if (exec_state.get() == NULL) ThrowException("Invalid handle");

  RecordServiceTaskState* task_state = exec_state->record_service_task_state();
  exec_state->BlockOnWait();

  task_state->results->SetReturnBuffer(&return_val);

  lock_guard<mutex> frl(*exec_state->fetch_rows_lock());
  lock_guard<mutex> l(*exec_state->lock());

  Status status = exec_state->FetchRows(
      task_state->fetch_size, task_state->results.get());
  if (!status.ok()) ThrowException(status.GetErrorMsg());
  return_val.done = exec_state->eos();
  return_val.row_batch_format = task_state->format;

  task_state->results->FinalizeResult();
}

void ImpalaServer::CloseTask(const recordservice::TUniqueId& req) {
  TUniqueId query_id;
  query_id.hi = req.hi;
  query_id.lo = req.lo;

  shared_ptr<QueryExecState> exec_state = GetQueryExecState(query_id, false);
  if (exec_state.get() == NULL) return;
  Status status = CancelInternal(query_id, true);
  if (!status.ok()) return;
  UnregisterQuery(query_id, true);
}

// Macros to convert from runtime profile counters to metrics object.
// Also does unit conversion (i.e. STAT_MS converts to millis).
#define SET_STAT_FROM_PROFILE(profile, counter_name, stats, stat_name)\
  do {\
    RuntimeProfile::Counter* c = profile->GetCounter(counter_name);\
    if (c != NULL) stats.__set_##stat_name(c->value());\
  } while(false)

#define SET_STAT_MS_FROM_PROFILE(profile, counter_name, stats, stat_name)\
  do {\
    RuntimeProfile::Counter* c = profile->GetCounter(counter_name);\
    if (c != NULL) stats.__set_##stat_name(c->value() / 1000000);\
  } while(false)

void ImpalaServer::GetTaskStats(recordservice::TStats& return_val,
      const recordservice::TUniqueId& req) {
  TUniqueId query_id;
  query_id.hi = req.hi;
  query_id.lo = req.lo;

  shared_ptr<QueryExecState> exec_state = GetQueryExecState(query_id, false);
  if (exec_state.get() == NULL) ThrowException("Invalid handle");

  lock_guard<mutex> l(*exec_state->lock());
  Coordinator* coord = exec_state->coord();
  if (coord == NULL) ThrowException("Invalid handle");

  // Extract counters from runtime profile. This is a hack too.
  RuntimeProfile* server_profile = exec_state->server_profile();
  SET_STAT_MS_FROM_PROFILE(server_profile, "RowMaterializationTimer",
      return_val, serialize_time_ms);
  SET_STAT_MS_FROM_PROFILE(server_profile, "ClientFetchWaitTimer",
      return_val, client_time_ms);

  RuntimeProfile* coord_profile = coord->query_profile();
  vector<RuntimeProfile*> children;
  coord_profile->GetAllChildren(&children);
  for (int i = 0; i < children.size(); ++i) {
    if (children[i]->name() != "HDFS_SCAN_NODE (id=0)") continue;
    RuntimeProfile* profile = children[i];

    RuntimeProfile::Counter* bytes_assigned = profile->GetCounter("BytesAssigned");
    RuntimeProfile::Counter* bytes_read = profile->GetCounter("BytesRead");
    if (bytes_read != NULL) {
      return_val.__set_bytes_read(bytes_read->value());
      if (bytes_assigned != NULL) {
        double percent = (double)bytes_read->value() / bytes_assigned->value();
        // This can happen because we read past the end of ranges to finish them.
        if (percent > 1) percent = 1;
        return_val.__set_completion_percentage(percent * 100);
      }
    }

    SET_STAT_FROM_PROFILE(profile, "BytesReadLocal", return_val, bytes_read_local);
    SET_STAT_FROM_PROFILE(profile, "RowsRead", return_val, num_rows_read);
    SET_STAT_FROM_PROFILE(profile, "RowsReturned", return_val, num_rows_returned);
    SET_STAT_MS_FROM_PROFILE(profile, "DecompressionTime", return_val,
        decompress_time_ms);
    SET_STAT_FROM_PROFILE(profile, "PerReadThreadRawHdfsThroughput", return_val,
        hdfs_throughput);
  }
}

}
