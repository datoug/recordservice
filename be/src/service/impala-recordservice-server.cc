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
#include "exprs/slot-ref.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/raw-value.h"
#include "service/query-exec-state.h"
#include "service/query-options.h"
#include "rpc/thrift-util.h"
#include "util/codec.h"
#include "util/debug-util.h"
#include "util/hdfs-util.h"
#include "util/recordservice-metrics.h"
#include "service/hs2-util.h"

using namespace std;
using namespace boost;
using namespace strings;
using namespace beeswax; // Converting QueryState
using namespace apache::thrift;

DECLARE_int32(recordservice_worker_port);

// This value has a big impact on performance. For simple queries (1 bigint col),
// 5000 is a 2x improvement over a fetch size of 1024.
// TODO: investigate more
static const int DEFAULT_FETCH_SIZE = 5000;

// Names of temporary tables used to service path based requests.
// FIXME: everything about temp tables is a hack.
static const char* TEMP_DB = "rs_tmp_db";
static const char* TEMP_TBL = "tmp_tbl";

namespace impala {

void ImpalaServer::ThrowRecordServiceException(
    const recordservice::TErrorCode::type& code,
    const string& msg, const string& detail) {
  recordservice::TRecordServiceException ex;
  ex.code = code;
  ex.message = msg;
  if (!detail.empty()) ex.__set_detail(detail);
  throw ex;
}

inline void ThrowFetchException(const Status& status) {
  DCHECK(!status.ok());
  recordservice::TRecordServiceException ex;
  if (status.IsCancelled()) {
    ex.code = recordservice::TErrorCode::CANCELLED;
    ex.message = "Task failed because it was cancelled.";
  } else if (status.IsMemLimitExceeded()) {
    ex.code = recordservice::TErrorCode::OUT_OF_MEMORY;
    ex.message = "Task failed because it ran out of memory.";
  } else {
    ex.code = recordservice::TErrorCode::INTERNAL_ERROR;
    ex.message = "Task failed due to an internal error.";
  }
  ex.__set_detail(status.msg().GetFullMessageDetails());
  throw ex;
}

// Base class for test result set serializations. The functions in here and
// not used in the RecordService path.
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
      if (types_[i] == TYPE_TIMESTAMP) {
        type_sizes_.push_back(12);
      } else {
        type_sizes_.push_back(types_[i].GetByteSize());
      }
    }
  }

  virtual void FinalizeResult() {}

  virtual bool supports_batch_add() const { return true; }

  // This should be set for every fetch request so that the results are directly
  // populated in the thrift result object (to avoid a copy).
  virtual void SetReturnBuffer(recordservice::TFetchResult* result) = 0;

  virtual size_t size() { return result_ == NULL ? 0 : result_->num_records; }

 protected:
  BaseResultSet() : result_(NULL) {}
  recordservice::TFetchResult* result_;

  vector<ColumnType> types_;
  vector<int> type_sizes_;
};

// Additional state for the RecordService. Put here instead of QueryExecState
// to separate from Impala code.
class ImpalaServer::RecordServiceTaskState {
 public:
  RecordServiceTaskState() : counters_initialized(false) {}

  int fetch_size;

  recordservice::TRecordFormat::type format;
  scoped_ptr<ImpalaServer::BaseResultSet> results;

  // Populated on first call to Fetch(). At that point the query has for sure
  // made enough progress that the counters are initialized.
  bool counters_initialized;
  RuntimeProfile::Counter* serialize_timer;
  RuntimeProfile::Counter* client_timer;

  RuntimeProfile::Counter* bytes_assigned_counter;
  RuntimeProfile::Counter* bytes_read_counter;
  RuntimeProfile::Counter* bytes_read_local_counter;
  RuntimeProfile::Counter* rows_read_counter;
  RuntimeProfile::Counter* rows_returned_counter;
  RuntimeProfile::Counter* decompression_timer;
  RuntimeProfile::Counter* hdfs_throughput_counter;
};

// This is the parquet plain encoding, meaning we append the little endian version
// of the value to the end of the buffer.
class ImpalaServer::RecordServiceParquetResultSet : public ImpalaServer::BaseResultSet {
 public:
  RecordServiceParquetResultSet(bool all_slot_refs,
      const vector<ExprContext*>& output_exprs) : all_slot_refs_(all_slot_refs) {
    if (all_slot_refs) {
      slot_descs_.resize(output_exprs.size());
      for (int i = 0; i < output_exprs.size(); ++i) {
        SlotRef* slot_ref = reinterpret_cast<SlotRef*>(output_exprs[i]->root());
        slot_descs_[i].byte_offset = slot_ref->slot_offset();
        slot_descs_[i].null_offset = slot_ref->null_indicator();
      }
    }
  }

  virtual void SetReturnBuffer(recordservice::TFetchResult* result) {
    result_ = result;
    result_->__isset.columnar_records = true;
    result_->columnar_records.cols.resize(types_.size());
  }

  virtual void AddRowBatch(RowBatch* input, int row_idx, int num_rows,
      vector<ExprContext*>* ctxs) {
    DCHECK(result_->__isset.columnar_records);
    recordservice::TColumnarRecords& batch = result_->columnar_records;

    if (all_slot_refs_) {
      // In this case, all the output exprs are slot refs and we want to serialize them
      // to the RecordService format. To do this we:
      // 1. Reserve the outgoing buffer to the max size (for fixed length types).
      // 2. Append the current value to the outgoing buffer.
      // 3. Resize the outgoing buffer when we are done with the row batch (which
      // can be sparse due to NULLs).
      DCHECK_EQ(ctxs->size(), slot_descs_.size());
      const int num_cols = slot_descs_.size();

      // Reserve the size of the output where possible.
      for (int c = 0; c < num_cols; ++c) {
        DCHECK_EQ(batch.cols[c].is_null.size(), 0);
        DCHECK_EQ(batch.cols[c].data.size(), 0);

        batch.cols[c].is_null.resize(num_rows);
        if (type_sizes_[c] != 0) {
          batch.cols[c].data.resize(num_rows * type_sizes_[c]);
        }
      }

      // Only used for fixed length types. data[c] is the ptr that the next value
      // should be appended at.
      char* data[num_cols];
      for (int c = 0; c < num_cols; ++c) {
        data[c] = (char*)batch.cols[c].data.data();
      }

      // This loop is extremely perf sensitive.
      for (int i = 0; i < num_rows; ++i) {
        Tuple* tuple = input->GetRow(row_idx++)->GetTuple(0);
        for (int c = 0; c < num_cols; ++c) {
          bool is_null = tuple->IsNull(slot_descs_[c].null_offset);
          batch.cols[c].is_null[i] = is_null;
          if (is_null) continue;

          const int type_size = type_sizes_[c];
          if (type_size == 0) {
            // TODO: this resizing can't be good. The rowbatch should keep track of
            // how long the string data is.
            string& dst = batch.cols[c].data;
            int offset = dst.size();
            const StringValue* sv = tuple->GetStringSlot(slot_descs_[c].byte_offset);
            int len = sv->len + sizeof(int32_t);
            dst.resize(offset + len);
            memcpy((char*)dst.data() + offset, &sv->len, sizeof(int32_t));
            memcpy((char*)dst.data() + offset + sizeof(int32_t), sv->ptr, sv->len);
          } else {
            const void* slot = tuple->GetSlot(slot_descs_[c].byte_offset);
            if (types_[c] == TYPE_TIMESTAMP) {
              DCHECK_EQ(type_size, 12);
              const TimestampValue* ts = reinterpret_cast<const TimestampValue*>(slot);
              int64_t millis;
              int32_t nanos;
              ts->ToMillisAndNanos(&millis, &nanos);
              memcpy(data[c], &millis, sizeof(int64_t));
              memcpy(data[c] + sizeof(int64_t), &nanos, sizeof(int32_t));
            } else {
              memcpy(data[c], slot, type_size);
            }
            data[c] += type_size;
          }
        }
      }

      // For fixed-length columns, shrink the size if necessary. In the case of NULLs,
      // we could have resized the buffer bigger than necessary.
      for (int c = 0; c < num_cols; ++c) {
        if (type_sizes_[c] == 0) continue;
        int size = data[c] - batch.cols[c].data.data();
        if (batch.cols[c].data.size() != size) {
          batch.cols[c].data.resize(size);
        }
      }
    } else {
      // Reserve the size of the output where possible.
      for (int c = 0; c < ctxs->size(); ++c) {
        DCHECK_EQ(batch.cols[c].is_null.size(), 0);
        DCHECK_EQ(batch.cols[c].data.size(), 0);

        batch.cols[c].is_null.reserve(num_rows);
        if (type_sizes_[c] != 0) {
          batch.cols[c].data.reserve(num_rows * type_sizes_[c]);
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
    }
    result_->num_records += num_rows;
  }

 private:
  struct SlotDesc {
    // Byte offset in tuple
    int byte_offset;
    NullIndicatorOffset null_offset;

    SlotDesc() : null_offset(0, 0) {}
  };

  // If true, all the output exprs are slot refs.
  bool all_slot_refs_;

  // Cache of the slot desc. Only set if all_slot_refs_ is true. We'll use this
  // instead of the exprs (for performance).
  vector<SlotDesc> slot_descs_;
};

void ImpalaServer::GetRecordServiceSession(ScopedSessionState* session) {
  Status status = session->WithSession(ThriftServer::GetThreadConnectionId());
  if (!status.ok()) {
    // The session is tied to the thrift connection so the only way this can
    // happen is if the server timed out the session.
    ThrowRecordServiceException(recordservice::TErrorCode::CONNECTION_TIMED_OUT,
        "Connection has timed out. Reconnect to the server.");
  }
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
    case TYPE_VARCHAR:
      result.type_id = recordservice::TTypeId::VARCHAR;
      result.__set_len(t.len);
      break;
    case TYPE_CHAR:
      result.type_id = recordservice::TTypeId::CHAR;
      result.__set_len(t.len);
      break;
    case TYPE_TIMESTAMP:
      result.type_id = recordservice::TTypeId::TIMESTAMP_NANOS;
      break;
    case TYPE_DECIMAL:
      result.type_id = recordservice::TTypeId::DECIMAL;
      result.__set_precision(t.precision);
      result.__set_scale(t.scale);
      break;
    default:
      ImpalaServer::ThrowRecordServiceException(
          recordservice::TErrorCode::INVALID_REQUEST, "Not supported type.");
  }
  return result;
}

static void PopulateResultSchema(const TResultSetMetadata& metadata,
    recordservice::TSchema* schema) {
  schema->cols.resize(metadata.columns.size());
  for (int i = 0; i < metadata.columns.size(); ++i) {
    ColumnType type(metadata.columns[i].columnType);
    schema->cols[i].type = ToRecordServiceType(type);
    schema->cols[i].name = metadata.columns[i].columnName;
  }
}

recordservice::TProtocolVersion::type ImpalaServer::GetProtocolVersion() {
  return recordservice::TProtocolVersion::V1;
}

TExecRequest ImpalaServer::PlanRecordServiceRequest(
    const recordservice::TPlanRequestParams& req) {
  RecordServiceMetrics::NUM_PLAN_REQUESTS->Increment(1);
  if (IsOffline()) {
    ThrowRecordServiceException(recordservice::TErrorCode::SERVICE_BUSY,
        "This RecordServicePlanner is not ready to accept requests."
        " Retry your request later.");
  }

  TQueryCtx query_ctx;
  query_ctx.__set_is_record_service_request(true);

  // Setting num_nodes = 1 means we generate a single node plan which has
  // a simpler structure. It also prevents Impala from analyzing multi-table
  // queries, i.e. joins.
  query_ctx.request.query_options.__set_num_nodes(1);

  // Disable codegen. Codegen works well for Impala because each fragment processes
  // multiple blocks, so the cost of codegen is amortized.
  // TODO: implement codegen caching.
  query_ctx.request.query_options.__set_disable_codegen(true);

  unique_lock<mutex> tmp_tbl_lock;

  switch (req.request_type) {
    case recordservice::TRequestType::Sql:
      query_ctx.request.stmt = req.sql_stmt;
      break;
    case recordservice::TRequestType::Path: {
      // TODO: improve tmp table management or get impala to do it properly.
      unique_lock<mutex> l(tmp_tbl_lock_);
      tmp_tbl_lock.swap(l);

      string tmp_table;
      Status status = CreateTmpTable(req.path, &tmp_table);
      if (!status.ok()) {
        ThrowRecordServiceException(recordservice::TErrorCode::INVALID_REQUEST,
            "Could not create temporary table.",
            status.msg().GetFullMessageDetails());
      }
      if (req.path.__isset.query) {
        string query = req.path.query;
        size_t p = req.path.query.find("__PATH__");
        if (p == string::npos) {
          ThrowRecordServiceException(recordservice::TErrorCode::INVALID_REQUEST,
              "Query request must contain __PATH__: " + query);
        }
        query.replace(p, 8, tmp_table);
        query_ctx.request.stmt = query;
      } else {
        query_ctx.request.stmt = "SELECT * FROM " + tmp_table;
      }
      break;
    }
    default:
      ThrowRecordServiceException(recordservice::TErrorCode::INVALID_REQUEST,
          "Unsupported request types. Supported request types are: SQL");
  }

  LOG(ERROR) << "RecordService::PlanRequest: " << query_ctx.request.stmt;

  // Plan the request.
  TExecRequest result;
  Status status = exec_env_->frontend()->GetRecordServiceExecRequest(query_ctx, &result);
  if (tmp_tbl_lock.owns_lock()) tmp_tbl_lock.unlock();
  if (!status.ok()) {
    ThrowRecordServiceException(recordservice::TErrorCode::INVALID_REQUEST,
        "Could not plan request.",
        status.msg().GetFullMessageDetails());
  }
  if (result.stmt_type != TStmtType::QUERY) {
    ThrowRecordServiceException(recordservice::TErrorCode::INVALID_REQUEST,
        "Cannot run non-SELECT statements");
  }
  return result;
}

//
// RecordServicePlanner
//
void ImpalaServer::PlanRequest(recordservice::TPlanRequestResult& return_val,
  const recordservice::TPlanRequestParams& req) {
  try {
    TExecRequest result = PlanRecordServiceRequest(req);

    // TODO: this port should come from the membership information and return all hosts
    // the workers are running on.
    recordservice::TNetworkAddress default_host;
    default_host.hostname = "localhost";
    default_host.port = FLAGS_recordservice_worker_port;
    return_val.hosts.push_back(default_host);

    // Extract the types of the result.
    DCHECK(result.__isset.result_set_metadata);
    PopulateResultSchema(result.result_set_metadata, &return_val.schema);

    // Walk the plan to compute the tasks. We want to find the scan ranges and
    // convert them into tasks.
    // Impala, for these queries, will generate one fragment (with a single scan node)
    // and have all the scan ranges in that scan node. We want to generate one task
    // (with the fragment) for each scan range.
    // TODO: this needs to be revisited. It scales very poorly. For a scan with 1M
    // blocks (64MB/block = ~61TB dataset), this generates 1M tasks. Even at 100B
    // tasks, this is a 100MB response.
    DCHECK(result.__isset.query_exec_request);
    TQueryExecRequest& query_request = result.query_exec_request;
    DCHECK_EQ(query_request.per_node_scan_ranges.size(), 1);
    vector<TScanRangeLocations> scan_ranges;
    const int64_t scan_node_id = query_request.per_node_scan_ranges.begin()->first;
    scan_ranges.swap(query_request.per_node_scan_ranges.begin()->second);

    // TODO: log audit events. Is there right? Should we log this on the worker?
    //if (IsAuditEventLoggingEnabled()) {
    //  LogAuditRecord(*(exec_state->get()), *(request));
    //}
    result.access_events.clear();

    // Send analysis warning as part of TPlanRequestResult.
    for (int i = 0; i < result.analysis_warnings.size(); ++i) {
      recordservice::TLogMessage msg;
      msg.message = result.analysis_warnings[i];
      return_val.warnings.push_back(msg);
    }
    result.analysis_warnings.clear();

    // Empty scan, just return. No tasks to generate.
    if (scan_ranges.empty()) return;

    // The TRecordServiceExecRequest will contain a bunch of TQueryRequest
    // objects.. each corresponding to a PlanFragment. This is then reconstituted
    // into a list of TExecRequests (again one for each PlanFragment). Each
    // TExecRequest is then serialized and set as the "task" field of the
    // TTask object.
    // To do this we:
    //  1. Copy the original request
    //  2. Modify it so it contains just enough information for the scan range it is for.
    //  3. Reserialize and compress it.
    // TODO : we would need to encrypt the TExecRequest object for security. The client
    // cannot tamper with the task object.
    int buffer_size = 100 * 1024;  // start out with 100KB
    ThriftSerializer serializer(true, buffer_size);

    scoped_ptr<Codec> compressor;
    Codec::CreateCompressor(NULL, false, THdfsCompression::LZ4, &compressor);

    // Collect all references partitions and remove them from the 'result' request
    // object. Each task will only reference a single partition and we don't want
    // to send the rest.
    map<int64_t, THdfsPartition> all_partitions;
    DCHECK_EQ(query_request.desc_tbl.tableDescriptors.size(), 1);
    if (query_request.desc_tbl.tableDescriptors[0].__isset.hdfsTable) {
      all_partitions.swap(
          query_request.desc_tbl.tableDescriptors[0].hdfsTable.partitions);
    }

    // Do the same for hosts.
    vector<TNetworkAddress> all_hosts;
    all_hosts.swap(query_request.host_list);

    for (int i = 0; i < scan_ranges.size(); ++i) {
      recordservice::TTask task;

      // Add the partition metadata.
      if (scan_ranges[i].scan_range.__isset.hdfs_file_split) {
        query_request.desc_tbl.tableDescriptors[0].hdfsTable.partitions.clear();
        int64_t id = scan_ranges[i].scan_range.hdfs_file_split.partition_id;
        query_request.desc_tbl.tableDescriptors[0].hdfsTable.partitions[id] =
            all_partitions[id];
      }

      // Populate the hosts.
      query_request.host_list.clear();
      for (int j = 0; j < scan_ranges[i].locations.size(); ++j) {
        TScanRangeLocation& loc = scan_ranges[i].locations[j];
        recordservice::TNetworkAddress host;
        DCHECK(all_hosts[loc.host_idx].__isset.hdfs_host_name);
        host.hostname = all_hosts[loc.host_idx].hdfs_host_name;
        // TODO: this port should come from the membership information.
        host.port = FLAGS_recordservice_worker_port;
        task.local_hosts.push_back(host);

        // Populate query_request.host_list and remap indices.
        query_request.host_list.push_back(all_hosts[loc.host_idx]);
        loc.host_idx = query_request.host_list.size() - 1;
      }

      // Add the scan range.
      query_request.per_node_scan_ranges.clear();
      query_request.per_node_scan_ranges[scan_node_id].push_back(scan_ranges[i]);

      string serialized_task;
      serializer.Serialize<TExecRequest>(&result, &serialized_task);
      compressor->Compress(serialized_task, true, &task.task);
      return_val.tasks.push_back(task);
    }
  } catch (const recordservice::TRecordServiceException& e) {
    RecordServiceMetrics::NUM_FAILED_PLAN_REQUESTS->Increment(1);
    throw e;
  }
}

void ImpalaServer::GetSchema(recordservice::TGetSchemaResult& return_val,
      const recordservice::TPlanRequestParams& req) {
  RecordServiceMetrics::NUM_GET_SCHEMA_REQUESTS->Increment(1);
  try {
    // TODO: fix this to not do the whole planning.
    TExecRequest result = PlanRecordServiceRequest(req);
    DCHECK(result.__isset.result_set_metadata);
    PopulateResultSchema(result.result_set_metadata, &return_val.schema);
  } catch (const recordservice::TRecordServiceException& e) {
    RecordServiceMetrics::NUM_FAILED_GET_SCHEMA_REQUESTS->Increment(1);
    throw e;
  }
}

//
// RecordServiceWorker
//
void ImpalaServer::ExecTask(recordservice::TExecTaskResult& return_val,
    const recordservice::TExecTaskParams& req) {
  RecordServiceMetrics::NUM_TASK_REQUESTS->Increment(1);
  try {
    if (IsOffline()) {
      ThrowRecordServiceException(recordservice::TErrorCode::SERVICE_BUSY,
          "This RecordServicePlanner is not ready to accept requests."
          " Retry your request later.");
    }

    ScopedSessionState session_handle(this);
    GetRecordServiceSession(&session_handle);

    shared_ptr<RecordServiceTaskState> task_state(new RecordServiceTaskState());

    scoped_ptr<Codec> decompressor;
    Codec::CreateDecompressor(NULL, false, THdfsCompression::LZ4, &decompressor);
    string decompressed_task;
    Status status = decompressor->Decompress(req.task, true, &decompressed_task);
    if (!status.ok()) {
      ThrowRecordServiceException(recordservice::TErrorCode::INVALID_TASK,
          "Task is corrupt.",
          status.msg().GetFullMessageDetails());
    }

    TExecRequest exec_req;
    uint32_t size = decompressed_task.size();
    status = DeserializeThriftMsg(
        reinterpret_cast<const uint8_t*>(decompressed_task.data()),
        &size, true, &exec_req);
    if (!status.ok()) {
      ThrowRecordServiceException(recordservice::TErrorCode::INVALID_TASK,
          "Task is corrupt.",
          status.msg().GetFullMessageDetails());
    }
    LOG(INFO) << "RecordService::ExecRequest: query plan " <<
            exec_req.query_exec_request.query_plan;

    task_state->fetch_size = DEFAULT_FETCH_SIZE;
    if (req.__isset.fetch_size) task_state->fetch_size = req.fetch_size;
    exec_req.query_exec_request.
        query_ctx.request.query_options.__set_batch_size(task_state->fetch_size);
    if (req.__isset.mem_limit) {
      // FIXME: this needs much more testing.
      exec_req.query_exec_request.query_ctx.
        request.query_options.__set_mem_limit(req.mem_limit);
    }

    shared_ptr<QueryExecState> exec_state;
    status = ExecuteRecordServiceRequest(
            &exec_req.query_exec_request.query_ctx,
            &exec_req, session_handle.get(), &exec_state);
    if (!status.ok()) {
      ThrowRecordServiceException(recordservice::TErrorCode::INVALID_TASK,
          "Could not execute task.",
          status.msg().GetFullMessageDetails());
    }

    PopulateResultSchema(*exec_state->result_metadata(), &return_val.schema);
    exec_state->SetRecordServiceTaskState(task_state);

    // Optimization if the result exprs are all just "simple" slot refs. This means
    // that they contain a single non-nullable tuple row. This is the common case and
    // we can simplify the row serialization logic.
    // TODO: this should be replaced by codegen to handle all the cases.
    bool all_slot_refs = true;
    const vector<ExprContext*>& output_exprs = exec_state->output_exprs();
    for (int i = 0; i < output_exprs.size(); ++i) {
      if (output_exprs[i]->root()->is_slotref()) {
        SlotRef* slot_ref = reinterpret_cast<SlotRef*>(output_exprs[i]->root());
        if (!slot_ref->tuple_is_nullable() && slot_ref->tuple_idx() == 0) continue;
      }
      all_slot_refs = false;
      break;
    }

    task_state->format = recordservice::TRecordFormat::Columnar;
    if (req.__isset.record_format) task_state->format = req.record_format;
    switch (task_state->format) {
      case recordservice::TRecordFormat::Columnar:
        task_state->results.reset(new RecordServiceParquetResultSet(
            all_slot_refs, output_exprs));
        break;
      default:
        ThrowRecordServiceException(recordservice::TErrorCode::INVALID_REQUEST,
            "Service does not support this record format.");
    }
    task_state->results->Init(*exec_state->result_metadata(), task_state->fetch_size);

    exec_state->UpdateQueryState(QueryState::RUNNING);
    exec_state->WaitAsync();
    status = SetQueryInflight(session_handle.get(), exec_state);
    if (!status.ok()) {
      UnregisterQuery(exec_state->query_id(), false, &status);
    }
    return_val.handle.hi = exec_state->query_id().hi;
    return_val.handle.lo = exec_state->query_id().lo;
  } catch (const recordservice::TRecordServiceException& e) {
    RecordServiceMetrics::NUM_FAILED_TASK_REQUESTS->Increment(1);
    throw e;
  }
}

// Computes the percent of num/denom
static double ComputeCompletionPercentage(RuntimeProfile::Counter* num,
    RuntimeProfile::Counter* denom) {
  if (num == NULL || denom == NULL || denom->value() == 0) return 0;
  double result = (double)num->value() / (double)denom->value();
  if (result > 1) result = 1;
  return result * 100;
}

void ImpalaServer::Fetch(recordservice::TFetchResult& return_val,
    const recordservice::TFetchParams& req) {
  RecordServiceMetrics::NUM_FETCH_REQUESTS->Increment(1);
  try {
    TUniqueId query_id;
    query_id.hi = req.handle.hi;
    query_id.lo = req.handle.lo;

    shared_ptr<QueryExecState> exec_state = GetQueryExecState(query_id, false);
    if (exec_state.get() == NULL) {
      ThrowRecordServiceException(recordservice::TErrorCode::INVALID_HANDLE,
          "Invalid handle");
    }

    RecordServiceTaskState* task_state = exec_state->record_service_task_state();
    exec_state->BlockOnWait();

    task_state->results->SetReturnBuffer(&return_val);

    lock_guard<mutex> frl(*exec_state->fetch_rows_lock());
    lock_guard<mutex> l(*exec_state->lock());

    Status status = exec_state->FetchRows(
        task_state->fetch_size, task_state->results.get());
    if (!status.ok()) ThrowFetchException(status);

    if (!task_state->counters_initialized) {
      // First time the client called fetch. Extract the counters.
      RuntimeProfile* server_profile = exec_state->server_profile();

      task_state->serialize_timer = server_profile->GetCounter("RowMaterializationTimer");
      task_state->client_timer = server_profile->GetCounter("ClientFetchWaitTimer");

      RuntimeProfile* coord_profile = exec_state->coord()->query_profile();
      vector<RuntimeProfile*> children;
      coord_profile->GetAllChildren(&children);
      for (int i = 0; i < children.size(); ++i) {
        if (children[i]->name() != "HDFS_SCAN_NODE (id=0)") continue;
        RuntimeProfile* profile = children[i];
        task_state->bytes_assigned_counter = profile->GetCounter("BytesAssigned");
        task_state->bytes_read_counter = profile->GetCounter("BytesRead");
        task_state->bytes_read_local_counter = profile->GetCounter("BytesReadLocal");
        task_state->rows_read_counter = profile->GetCounter("RowsRead");
        task_state->rows_returned_counter = profile->GetCounter("RowsReturned");
        task_state->decompression_timer = profile->GetCounter("DecompressionTime");
        task_state->hdfs_throughput_counter =
          profile->GetCounter("PerReadThreadRawHdfsThroughput");
      }
      task_state->counters_initialized = true;
    }

    return_val.done = exec_state->eos();
    return_val.task_completion_percentage = ComputeCompletionPercentage(
        task_state->bytes_read_counter, task_state->bytes_assigned_counter);
    return_val.record_format = task_state->format;

    task_state->results->FinalizeResult();
    RecordServiceMetrics::NUM_ROWS_FETCHED->Increment(return_val.num_records);
  } catch (const recordservice::TRecordServiceException& e) {
    RecordServiceMetrics::NUM_FAILED_FETCH_REQUESTS->Increment(1);
    throw e;
  }
}

void ImpalaServer::CloseTask(const recordservice::TUniqueId& req) {
  TUniqueId query_id;
  query_id.hi = req.hi;
  query_id.lo = req.lo;

  shared_ptr<QueryExecState> exec_state = GetQueryExecState(query_id, false);
  if (exec_state.get() == NULL) return;

  RecordServiceMetrics::NUM_CLOSED_TASKS->Increment(1);
  Status status = CancelInternal(query_id, true);
  if (!status.ok()) return;
  UnregisterQuery(query_id, true);
}

// Macros to convert from runtime profile counters to metrics object.
// Also does unit conversion (i.e. STAT_MS converts to millis).
#define SET_STAT_MS_FROM_COUNTER(counter, stat_name)\
  if (counter != NULL) return_val.stats.__set_##stat_name(counter->value() / 1000000)

#define SET_STAT_FROM_COUNTER(counter, stat_name)\
  if (counter != NULL) return_val.stats.__set_##stat_name(counter->value())

// TODO: send back warnings from the runtime state. Impala doesn't generate them
// in the most useful way right now. Fix that.
void ImpalaServer::GetTaskStatus(recordservice::TTaskStatus& return_val,
      const recordservice::TUniqueId& req) {
  RecordServiceMetrics::NUM_GET_TASK_STATUS_REQUESTS->Increment(1);
  try {
    TUniqueId query_id;
    query_id.hi = req.hi;
    query_id.lo = req.lo;

    // TODO: should this grab the lock in GetQueryExecState()?
    shared_ptr<QueryExecState> exec_state = GetQueryExecState(query_id, false);
    if (exec_state.get() == NULL) {
      ThrowRecordServiceException(recordservice::TErrorCode::INVALID_HANDLE,
          "Invalid handle");
    }

    lock_guard<mutex> l(*exec_state->lock());

    RecordServiceTaskState* task_state = exec_state->record_service_task_state();
    if (!task_state->counters_initialized) {
      // Task hasn't started enough to have counters.
      return;
    }

    // Populate the results from the counters.
    return_val.stats.__set_completion_percentage(ComputeCompletionPercentage(
        task_state->bytes_read_counter, task_state->bytes_assigned_counter));
    SET_STAT_MS_FROM_COUNTER(task_state->serialize_timer, serialize_time_ms);
    SET_STAT_MS_FROM_COUNTER(task_state->client_timer, client_time_ms);
    SET_STAT_FROM_COUNTER(task_state->bytes_read_counter, bytes_read);
    SET_STAT_FROM_COUNTER(task_state->bytes_read_local_counter, bytes_read_local);
    SET_STAT_FROM_COUNTER(task_state->rows_read_counter, num_records_read);
    SET_STAT_FROM_COUNTER(task_state->rows_returned_counter, num_records_returned);
    SET_STAT_MS_FROM_COUNTER(task_state->decompression_timer, decompress_time_ms);
    SET_STAT_FROM_COUNTER(task_state->hdfs_throughput_counter, hdfs_throughput);
  } catch (const recordservice::TRecordServiceException& e) {
    RecordServiceMetrics::NUM_FAILED_GET_TASK_STATUS_REQUESTS->Increment(1);
    throw e;
  }
}

Status ImpalaServer::CreateTmpTable(const recordservice::TPathRequest& request,
    string* table_name) {
  hdfsFS fs;
  Status status = HdfsFsCache::instance()->GetDefaultConnection(&fs);
  if (!status.ok()) {
    // TODO: more error detail
    ThrowRecordServiceException(recordservice::TErrorCode::INTERNAL_ERROR,
        "Could not connect to HDFS");
  }

  // FIXME: this should do better globbing.
  string path = request.path;
  if (path[path.size() - 1] == '*') {
    path = path.substr(0, path.size() - 1);
  }

  bool is_directory = false;
  status = IsDirectory(fs, path.c_str(), &is_directory);
  if (!status.ok()) {
    ThrowRecordServiceException(recordservice::TErrorCode::INVALID_REQUEST,
        "No such file or directory: " + path);
  }

  if (!is_directory) {
    // TODO: Impala should support LOCATIONs that are not directories.
    ThrowRecordServiceException(recordservice::TErrorCode::INVALID_REQUEST,
        "Path must be a directory: " + path);
  }

  ScopedSessionState session_handle(this);
  GetRecordServiceSession(&session_handle);

  stringstream tbl_name;
  tbl_name << TEMP_DB << "." << TEMP_TBL;
  *table_name = tbl_name.str();

  // For now, we'll just always use one temp table.
  string commands[] = {
    "DROP TABLE IF EXISTS " + *table_name,
    "CREATE DATABASE IF NOT EXISTS " + string(TEMP_DB),
    // FIXME: assume text for now. How do we best handle this?
    "CREATE EXTERNAL TABLE " + *table_name +
        "(record STRING) LOCATION \"" + path + "\"",
  };

  int num_commands = sizeof(commands) / sizeof(commands[0]);
  for (int i = 0; i < num_commands; ++i) {
    TQueryCtx query_ctx;
    query_ctx.request.stmt = commands[i];

    shared_ptr<QueryExecState> exec_state;
    RETURN_IF_ERROR(Execute(&query_ctx, session_handle.get(), &exec_state));
    exec_state->UpdateQueryState(QueryState::RUNNING);

    Status status = SetQueryInflight(session_handle.get(), exec_state);
    if (!status.ok()) {
      UnregisterQuery(exec_state->query_id(), false, &status);
      return status;
    }

    // block until results are ready
    exec_state->Wait();
    status = exec_state->query_status();
    if (!status.ok()) {
      UnregisterQuery(exec_state->query_id(), false, &status);
      return status;
    }
    exec_state->UpdateQueryState(QueryState::FINISHED);
    UnregisterQuery(exec_state->query_id(), true);
  }

  return Status::OK;
}

}
