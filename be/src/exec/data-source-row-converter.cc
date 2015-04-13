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

#include "exec/data-source-row-converter.h"

#include <gutil/strings/substitute.h>

#include "exec/parquet-common.h"
#include "exec/read-write-util.h"

#include "common/names.h"

using namespace strings;
using namespace impala;
using namespace impala::extdatasource;

// $0 = num expected cols, $1 = actual num columns
const string ERROR_NUM_COLUMNS = "Data source returned unexpected number of columns. "
    "Expected $0 but received $1. This likely indicates a problem with the data source "
    "library.";
const string ERROR_MISMATCHED_COL_SIZES = "Data source returned columns containing "
    "different numbers of rows. This likely indicates a problem with the data source "
    "library.";
// $0 = column type (e.g. INT)
const string ERROR_INVALID_COL_DATA = "Data source returned inconsistent column data. "
    "Expected value of type $0 based on column metadata. This likely indicates a "
    "problem with the data source library.";
const string ERROR_INVALID_TIMESTAMP = "Data source returned invalid timestamp data. "
    "This likely indicates a problem with the data source library.";
const string ERROR_INVALID_DECIMAL = "Data source returned invalid decimal data. "
    "This likely indicates a problem with the data source library.";

// Size of an encoded TIMESTAMP
const size_t TIMESTAMP_SIZE = sizeof(int64_t) + sizeof(int32_t);

DataSourceRowConverter::DataSourceRowConverter(
    const TupleDescriptor* tuple_desc, const Tuple* template_tuple,
    const vector<SlotDescriptor*>& materialized_slots)
    : next_row_idx_(0), num_rows_(0), tuple_desc_(tuple_desc),
      template_tuple_(template_tuple),
      materialized_slots_(materialized_slots), row_batch_(NULL) {
  cols_next_val_idx_.resize(materialized_slots_.size(), 0);
}

// Sets the decimal value in the slot. Inline method to avoid nested switch statements.
inline Status SetDecimalVal(const ColumnType& type, char* bytes, int len,
    void* slot) {
  uint8_t* buffer = reinterpret_cast<uint8_t*>(bytes);
  switch (type.GetByteSize()) {
    case 4: {
      Decimal4Value* val = reinterpret_cast<Decimal4Value*>(slot);
      if (len > sizeof(Decimal4Value)) return Status(ERROR_INVALID_DECIMAL);
      // TODO: Move Decode() to a more generic utils class (here and below)
      ParquetPlainEncoder::Decode(buffer, len, val);
    }
    case 8: {
      Decimal8Value* val = reinterpret_cast<Decimal8Value*>(slot);
      if (len > sizeof(Decimal8Value)) return Status(ERROR_INVALID_DECIMAL);
      ParquetPlainEncoder::Decode(buffer, len, val);
      break;
    }
    case 16: {
      Decimal16Value* val = reinterpret_cast<Decimal16Value*>(slot);
      if (len > sizeof(Decimal16Value)) return Status(ERROR_INVALID_DECIMAL);
      ParquetPlainEncoder::Decode(buffer, len, val);
      break;
    }
    default: DCHECK(false);
  }
  return Status::OK;
}

Status DataSourceRowConverter::ResetRowBatch(
    const extdatasource::TRowBatch* const row_batch, bool verify) {
  row_batch_ = row_batch;
  fill(cols_next_val_idx_.begin(), cols_next_val_idx_.end(), 0);
  next_row_idx_ = 0;

  if (verify) {
    const vector<TColumnData>& cols = row_batch_->cols;
    if (materialized_slots_.size() != cols.size()) {
      return Status(Substitute(
          ERROR_NUM_COLUMNS, materialized_slots_.size(), cols.size()));
    }

    num_rows_ = -1;
    // If num_rows was set, use that, otherwise we set it to be the number of rows in
    // the first TColumnData and then ensure the number of rows in other columns are
    // consistent.
    if (row_batch_->__isset.num_rows) num_rows_ = row_batch_->num_rows;
    for (int i = 0; i < materialized_slots_.size(); ++i) {
      const TColumnData& col_data = cols[i];
      if (num_rows_ < 0) num_rows_ = col_data.is_null.size();
      if (num_rows_ != col_data.is_null.size()) return Status(ERROR_MISMATCHED_COL_SIZES);
    }
  }
  return Status::OK;
}

Status DataSourceRowConverter::MaterializeNextRow(Tuple* tuple, MemPool* pool) {
  // Make sure row_batch_ has been reset
  DCHECK_NOTNULL(row_batch_);
  // Init the tuple.
  if (template_tuple_ != NULL) {
    memcpy(tuple, template_tuple_, tuple_desc_->byte_size());
  } else {
    memset(tuple, 0, sizeof(uint8_t) * tuple_desc_->num_null_bytes());
  }

  // TODO: error checking here is needed for ext-data-source, but not for
  // hdfs-hive-serde-scanner. We should rethink about this.
  for (int i = 0; i < materialized_slots_.size(); ++i) {
    const SlotDescriptor* slot_desc = materialized_slots_[i];
    void* slot = tuple->GetSlot(slot_desc->tuple_offset());
    const TColumnData& col = row_batch_->cols[i];

    if (col.is_null[next_row_idx_]) {
      tuple->SetNull(slot_desc->null_indicator_offset());
      continue;
    }

    // Get and increment the index into the values array (e.g. int_vals) for this col.
    int val_idx = cols_next_val_idx_[i]++;
    switch (slot_desc->type().type) {
      case TYPE_STRING: {
        if (val_idx >= col.string_vals.size()) {
          return Status(Substitute(ERROR_INVALID_COL_DATA, "STRING"));
        }
        const string& val = col.string_vals[val_idx];
        size_t val_size = val.size();
        char* buffer = reinterpret_cast<char*>(pool->Allocate(val_size));
        memcpy(buffer, val.data(), val_size);
        reinterpret_cast<StringValue*>(slot)->ptr = buffer;
        reinterpret_cast<StringValue*>(slot)->len = val_size;
        break;
      }
      case TYPE_TINYINT:
        if (val_idx >= col.byte_vals.size()) {
          return Status(Substitute(ERROR_INVALID_COL_DATA, "TINYINT"));
        }
        *reinterpret_cast<int8_t*>(slot) = col.byte_vals[val_idx];
        break;
      case TYPE_SMALLINT:
        if (val_idx >= col.short_vals.size()) {
          return Status(Substitute(ERROR_INVALID_COL_DATA, "SMALLINT"));
        }
        *reinterpret_cast<int16_t*>(slot) = col.short_vals[val_idx];
        break;
      case TYPE_INT:
        if (val_idx >= col.int_vals.size()) {
          return Status(Substitute(ERROR_INVALID_COL_DATA, "INT"));
        }
        *reinterpret_cast<int32_t*>(slot) = col.int_vals[val_idx];
        break;
      case TYPE_BIGINT:
        if (val_idx >= col.long_vals.size()) {
          return Status(Substitute(ERROR_INVALID_COL_DATA, "BIGINT"));
        }
        *reinterpret_cast<int64_t*>(slot) = col.long_vals[val_idx];
        break;
      case TYPE_DOUBLE:
        if (val_idx >= col.double_vals.size()) {
          return Status(Substitute(ERROR_INVALID_COL_DATA, "DOUBLE"));
        }
        *reinterpret_cast<double*>(slot) = col.double_vals[val_idx];
        break;
      case TYPE_FLOAT:
        if (val_idx >= col.double_vals.size()) {
          return Status(Substitute(ERROR_INVALID_COL_DATA, "FLOAT"));
        }
        *reinterpret_cast<float*>(slot) = col.double_vals[val_idx];
        break;
      case TYPE_BOOLEAN:
        if (val_idx >= col.bool_vals.size()) {
          return Status(Substitute(ERROR_INVALID_COL_DATA, "BOOLEAN"));
        }
        *reinterpret_cast<int8_t*>(slot) = col.bool_vals[val_idx];
        break;
      case TYPE_TIMESTAMP: {
        if (val_idx >= col.binary_vals.size()) {
          return Status(Substitute(ERROR_INVALID_COL_DATA, "TIMESTAMP"));
        }
        const string& val = col.binary_vals[val_idx];
        if (val.size() != TIMESTAMP_SIZE) return Status(ERROR_INVALID_TIMESTAMP);
        const uint8_t* bytes = reinterpret_cast<const uint8_t*>(val.data());
        *reinterpret_cast<TimestampValue*>(slot) = TimestampValue(
            ReadWriteUtil::GetInt<uint64_t>(bytes),
            ReadWriteUtil::GetInt<uint32_t>(bytes + sizeof(int64_t)));
        break;
      }
      case TYPE_DECIMAL: {
        if (val_idx >= col.binary_vals.size()) {
          return Status(Substitute(ERROR_INVALID_COL_DATA, "DECIMAL"));
        }
        const string& val = col.binary_vals[val_idx];
        RETURN_IF_ERROR(SetDecimalVal(slot_desc->type(), const_cast<char*>(val.data()),
            val.size(), slot));
        break;
      }
      default:
        DCHECK(false);
    }
  }

  ++next_row_idx_;
  return Status::OK;
}
