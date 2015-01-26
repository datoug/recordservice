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


#ifndef IMPALA_EXEC_RECORD_SERVICE_SCAN_NODE_H
#define IMPALA_EXEC_RECORD_SERVICE_SCAN_NODE_H

#include <vector>
#include <memory>
#include <stdint.h>

#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/thread/condition_variable.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>

#include "exec/scan-node.h"
#include "rpc/thrift-client.h"
#include "runtime/descriptors.h"
#include "util/progress-updater.h"
#include "util/spinlock.h"
#include "util/thread.h"

#include "gen-cpp/PlanNodes_types.h"
#include "gen-cpp/RecordServiceWorker.h"

namespace impala {

class DescriptorTbl;
class RowBatch;
class Status;
class Tuple;
class TPlanNode;

// A scan node that talks to the record service (over rpc) to return rows.
//
// This cannot be used by the record service itself (infinite loop).
class RecordServiceScanNode : public ScanNode {
 public:
  RecordServiceScanNode(ObjectPool* pool, const TPlanNode& tnode,
      const DescriptorTbl& descs);

  ~RecordServiceScanNode();

  // ExecNode methods
  virtual Status Prepare(RuntimeState* state);
  virtual Status Open(RuntimeState* state);
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos);
  virtual void Close(RuntimeState* state);

 private:
  // Tuple id resolved in Prepare() to set tuple_desc_;
  const int tuple_id_;

  // Descriptor for tuples this scan node constructs
  const TupleDescriptor* tuple_desc_;

  int tuple_byte_size_;

  // Descriptor for the hdfs table, including partition and format metadata.
  // Set in Prepare, owned by RuntimeState
  const HdfsTableDescriptor* hdfs_table_;

  std::vector<SlotDescriptor*> materialized_slots_;
  std::vector<std::string> materialized_col_names_;

  boost::scoped_ptr<ThriftClient<recordservice::RecordServiceWorkerClient> > rsw_client_;

  struct TaskState {
    std::string stmt;
    recordservice::TUniqueId handle;
    bool connected;

    TaskState() : connected(false) {}
  };

  // All the tasks (aka splits)
  std::vector<TaskState> tasks_;

  // current task we're on (starts at 0)
  int task_id_;

  inline Tuple* next_tuple(Tuple* t) const {
    uint8_t* mem = reinterpret_cast<uint8_t*>(t);
    return reinterpret_cast<Tuple*>(mem + tuple_byte_size_);
  }
};

}

#endif
