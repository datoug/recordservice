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

#ifndef IMPALA_EXEC_DATA_SOURCE_ROW_CONVERTER_H
#define IMPALA_EXEC_DATA_SOURCE_ROW_CONVERTER_H

#include <vector>

#include "common/status.h"
#include "runtime/descriptors.h"
#include "runtime/mem-pool.h"
#include "runtime/tuple.h"

#include "gen-cpp/ExternalDataSource_types.h"

namespace impala {

// A utility class to convert and materialize row batches from external
// data sources' serialization format to tuples.
class DataSourceRowConverter {
 public:
  // The input parameters need to live longer than this object.
  DataSourceRowConverter(const TupleDescriptor* tuple_desc,
                         const Tuple* template_tuple,
                         const std::vector<SlotDescriptor*>& materialized_slots);

  // Materializes the next row (next_row_idx_) into tuple. Init
  // the tuple with template_tuple_ beforehand.
  Status MaterializeNextRow(Tuple* tuple, MemPool* pool);

  // Reset the row batch to the input. If 'verify' is true, also checks if it
  // contains the correct number of columns and that columns contain the same
  // number of rows, and set num_rows_.
  // TODO: think how to unify the cases where data may come from an external data
  // source (and needs verify), or data may come from our controlled source.
  Status ResetRowBatch(const extdatasource::TRowBatch* const row_batch, bool verify);

  // Return false if there's no more row in the row batch.
  inline bool HasNextRow() {
    DCHECK_NOTNULL(row_batch_);
    // If the num_rows is not set, it means this row batch comes from
    // an external data source, and ResetRowBatch() should be called with
    // 'verify' set to true. Hence, the num_rows_ should also be set.
    return next_row_idx_ <
      (row_batch_->__isset.num_rows ? row_batch_->num_rows : num_rows_);
  }

 private:
  // The index of the next row in row_batch_, i.e. the index into TColumnData.is_null.
  int next_row_idx_;

  // The number of rows in row_batch_. The data source should have set
  // TRowBatch.num_rows, but we compute it just in case they haven't.
  int num_rows_;

  // Descriptor of tuples read. Used to initialize tuple with template_tuple.
  const TupleDescriptor* tuple_desc_;

  // A partially materialized tuple with only partition key slots set
  // The non-partition key slots are set to null. If there are no
  // partition key slots, this pointer is NULL.
  const Tuple* template_tuple_;

  // Columns to be materialized. This should have the same size as the # of
  // columns in the row_batch_.
  const std::vector<SlotDescriptor*>& materialized_slots_;

  // The external row batch that is being processed by this converter.
  // Every call to MaterializeNextRow will fetch one row from this and
  // materialize it to the tuple.
  const extdatasource::TRowBatch* row_batch_;

  // The indexes of the next non-null value in the row batch, per column.
  // Should always contain row_batch_.cols.size() integers. All values are reset to 0
  // when ResetRowBatch() is called.
  std::vector<int> cols_next_val_idx_;
};
}

#endif
