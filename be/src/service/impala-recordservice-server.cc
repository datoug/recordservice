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

// Result set conversion for record service.
// TODO: try different ways to do this. This current just counts.
class ImpalaServer::RecordServiceResultSet : public ImpalaServer::QueryResultSet {
 public:
  RecordServiceResultSet() : count_(0) {
  }

  virtual ~RecordServiceResultSet() { }

  virtual Status AddOneRow(const vector<void*>& col_values, const vector<int>& scales) {
    ++count_;
    return Status::OK;
  }

  // Convert TResultRow to ASCII using "\t" as column delimiter and store it in this
  // result set.
  virtual Status AddOneRow(const TResultRow& row) {
    ++count_;
    return Status::OK;
  }

  virtual int AddRows(const QueryResultSet* other, int start_idx, int num_rows) {
    count_ += num_rows;
    return num_rows;
  }

  virtual int64_t ByteSize(int start_idx, int num_rows) {
    return sizeof(int64_t);
  }

  virtual size_t size() { return count_; }

 private:
  int64_t count_;
};

void ImpalaServer::ExecRequest(recordservice::TExecRequestResult& return_val,
    const recordservice::TExecRequestParams& req) {

  {
    unique_lock<mutex> l(connection_to_sessions_map_lock_);
    if (record_service_session_.get() == NULL) {
      record_service_session_.reset(new SessionState());
      record_service_session_->start_time = TimestampValue::local_time();
      record_service_session_->last_accessed_ms = ms_since_epoch();
      record_service_session_->database = "default";
      record_service_session_->ref_count = 1;
    }
  }

  LOG(ERROR) << "RecordService::ExecRequest: " << req.request;
  TQueryCtx query_ctx;
  query_ctx.request.stmt = req.request;

  shared_ptr<QueryExecState> exec_state;
  Status status = Execute(&query_ctx, record_service_session_, &exec_state);
  if (!status.ok()) {
    recordservice::RecordServiceException ex;
    ex.message = status.GetErrorMsg();
    throw ex;
  }
  exec_state->UpdateQueryState(QueryState::RUNNING);
  exec_state->WaitAsync();
  status = SetQueryInflight(record_service_session_, exec_state);
  if (!status.ok()) {
    UnregisterQuery(exec_state->query_id(), false, &status);
  }
  return_val.handle.hi = exec_state->query_id().hi;
  return_val.handle.lo = exec_state->query_id().lo;
}

void ImpalaServer::GetCount(recordservice::TGetCountResult& return_val,
    const recordservice::TGetCountParams& req) {
  return_val.num_rows = 0;
  return_val.done = true;

  TUniqueId query_id;
  query_id.hi = req.handle.hi;
  query_id.lo = req.handle.lo;

  shared_ptr<QueryExecState> exec_state = GetQueryExecState(query_id, false);
  if (exec_state.get() == NULL) {
    recordservice::RecordServiceException ex;
    ex.message = "Invalid handle";
    throw ex;
  }

  exec_state->BlockOnWait();

  lock_guard<mutex> frl(*exec_state->fetch_rows_lock());
  lock_guard<mutex> l(*exec_state->lock());

  RecordServiceResultSet results;
  Status status = exec_state->FetchRows(1024, &results);
  if (!status.ok()) {
    recordservice::RecordServiceException ex;
    ex.message = status.GetErrorMsg();
    throw ex;
  }

  return_val.num_rows = results.size();
  return_val.done = exec_state->eos();
}

}
