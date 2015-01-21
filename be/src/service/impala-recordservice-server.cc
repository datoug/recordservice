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

namespace impala {

// Base class for test result set serializations. The functions in here and
// not used in the record service path.
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
};

// Result set conversion for record service.
class ImpalaServer::RecordServiceCountResultSet : public ImpalaServer::BaseResultSet {
 public:
  RecordServiceCountResultSet() : count_(0) { }

  virtual Status AddOneRow(const vector<void*>& col_values, const vector<int>& scales) {
    ++count_;
    return Status::OK;
  }

  virtual size_t size() { return count_; }

  void SetMetadata(const TResultSetMetadata& md) {}

 private:
  int64_t count_;
};

class ImpalaServer::RecordServiceColumnarResultSet : public ImpalaServer::BaseResultSet {
 public:
  RecordServiceColumnarResultSet() {}

  void SetMetadata(const TResultSetMetadata& md) {
    if (md.columns.empty()) return;

    batch_.__isset.cols = true;
    batch_.cols.resize(md.columns.size());

    for (int i = 0; i < md.columns.size(); ++i) {
      types_.push_back(md.columns[i].columnType);
      switch (types_[i].type) {
        case TYPE_BOOLEAN:
          batch_.cols[i].__isset.bool_vals = true;
          break;
        case TYPE_TINYINT:
          batch_.cols[i].__isset.byte_vals = true;
          break;
        case TYPE_SMALLINT:
          batch_.cols[i].__isset.short_vals = true;
          break;
        case TYPE_INT:
          batch_.cols[i].__isset.int_vals = true;
          break;
        case TYPE_BIGINT:
          batch_.cols[i].__isset.long_vals = true;
          break;
        case TYPE_FLOAT:
        case TYPE_DOUBLE:
          batch_.cols[i].__isset.double_vals = true;
          break;
        case TYPE_VARCHAR:
        case TYPE_STRING:
          batch_.cols[i].__isset.string_vals = true;
          break;
        case TYPE_TIMESTAMP:
        case TYPE_DECIMAL:
          batch_.cols[i].__isset.binary_vals = true;
          break;
        default:
          CHECK(false) << "not implemented";
      }
    }
  }

  virtual Status AddOneRow(const vector<void*>& col_values, const vector<int>& scales) {
    DCHECK_EQ(col_values.size(), types_.size());
    for (int i = 0; i < col_values.size(); ++i) {
      const void* v = col_values[i];
      batch_.cols[i].is_null.push_back(v == NULL);
      if (v == NULL) continue;

      switch (types_[i].type) {
        case TYPE_BOOLEAN:
          batch_.cols[i].bool_vals.push_back(*reinterpret_cast<const bool*>(v));
          break;
        case TYPE_TINYINT:
          batch_.cols[i].byte_vals.push_back(*reinterpret_cast<const uint8_t*>(v));
          break;
        case TYPE_SMALLINT:
          batch_.cols[i].short_vals.push_back(*reinterpret_cast<const int16_t*>(v));
          break;
        case TYPE_INT:
          batch_.cols[i].int_vals.push_back(*reinterpret_cast<const int32_t*>(v));
          break;
        case TYPE_BIGINT:
          batch_.cols[i].long_vals.push_back(*reinterpret_cast<const int64_t*>(v));
          break;
        case TYPE_FLOAT:
          batch_.cols[i].double_vals.push_back(*reinterpret_cast<const float*>(v));
          break;
        case TYPE_DOUBLE:
          batch_.cols[i].double_vals.push_back(*reinterpret_cast<const double*>(v));
          break;
        case TYPE_VARCHAR:
        case TYPE_STRING: {
          const StringValue* sv = reinterpret_cast<const StringValue*>(v);
          batch_.cols[i].string_vals.push_back(sv->DebugString());
          break;
        }
        case TYPE_TIMESTAMP:
          batch_.cols[i].binary_vals.push_back(string((const char*)v, 16));
          break;
        case TYPE_DECIMAL:
          batch_.cols[i].binary_vals.push_back(
              string((const char*)v, types_[i].GetByteSize()));
          break;
        default:
          CHECK(false) << "not implemented";
      }
    }
    ++batch_.num_rows;
    return Status::OK;
  }

  virtual size_t size() { return batch_.num_rows; }

  recordservice::TColumnarRowBatch batch_;

 private:
  vector<ColumnType> types_;
};

inline void ThrowException(const std::string msg) {
  recordservice::RecordServiceException ex;
  ex.message = msg;
  throw ex;
}

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

void ImpalaServer::ExecRequest(recordservice::TExecRequestResult& return_val,
    const recordservice::TExecRequestParams& req) {
  GetRecordServiceSession();

  LOG(ERROR) << "RecordService::ExecRequest: " << req.request;
  TQueryCtx query_ctx;
  query_ctx.request.stmt = req.request;

  shared_ptr<QueryExecState> exec_state;
  Status status = Execute(&query_ctx, record_service_session_, &exec_state);
  if (!status.ok()) ThrowException(status.GetErrorMsg());

  exec_state->UpdateQueryState(QueryState::RUNNING);
  exec_state->WaitAsync();
  status = SetQueryInflight(record_service_session_, exec_state);
  if (!status.ok()) {
    UnregisterQuery(exec_state->query_id(), false, &status);
  }
  return_val.handle.hi = exec_state->query_id().hi;
  return_val.handle.lo = exec_state->query_id().lo;
}


template<typename T>
bool ImpalaServer::GetInternal(const recordservice::TGetParams& req, T* results) {
  TUniqueId query_id;
  query_id.hi = req.handle.hi;
  query_id.lo = req.handle.lo;

  shared_ptr<QueryExecState> exec_state = GetQueryExecState(query_id, false);
  if (exec_state.get() == NULL) ThrowException("Invalid handle");

  exec_state->BlockOnWait();
  results->SetMetadata(*exec_state->result_metadata());

  lock_guard<mutex> frl(*exec_state->fetch_rows_lock());
  lock_guard<mutex> l(*exec_state->lock());

  Status status = exec_state->FetchRows(1024, results);
  if (!status.ok()) ThrowException(status.GetErrorMsg());
  return exec_state->eos();
}

void ImpalaServer::GetCount(recordservice::TGetCountResult& return_val,
    const recordservice::TGetParams& req) {
  RecordServiceCountResultSet results;
  return_val.done = GetInternal(req, &results);
  return_val.num_rows = results.size();
}

void ImpalaServer::GetColumnarBatch(recordservice::TColumnarRowBatch& return_val,
    const recordservice::TGetParams& req) {
  RecordServiceColumnarResultSet results;
  bool done = GetInternal(req, &results);
  return_val = results.batch_;
  return_val.done = done;
}

//
// RecordServicePlanner
//
void ImpalaServer::PlanRequest(recordservice::TPlanRequestResult& return_val,
  const recordservice::TPlanRequestParams& req) {
  ThrowException("Not implemented");
}

//
// RecordServiceWorker
//
void ImpalaServer::ExecTask(recordservice::TExecTaskResult& return_val,
    const recordservice::TExecTaskParams& req) {
  ThrowException("Not implemented");
}

void ImpalaServer::Fetch(recordservice::TColumnarRowBatch& return_val,
    const recordservice::TFetchParams& req) {
  ThrowException("Not implemented");
}
void ImpalaServer::CancelTask(const recordservice::TUniqueId& req) {
}

}
