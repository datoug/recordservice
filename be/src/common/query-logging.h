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

#ifndef IMPALA_COMMON_QUERY_LOGGING_H
#define IMPALA_COMMON_QUERY_LOGGING_H

#include <string>

#include "common/logging.h"
#include "gen-cpp/Types_types.h"  // for TUniqueId

// This contains macros and utilities to have different log levels per query.
// The key additions is to support levels per logger (as opposed to per process)
// and to prefix the logs with a fixed message (e.g. query id).

// TODO: we can redirect the log to a separate file (stored in the logger) if we
// want per file logging. We'd use that instead of the LOG(INFO) macro.
#define QUERY_VLOG(LOGGER, LEVEL)\
  !((LOGGER)->Enabled(LEVEL)) ?\
      (void) 0 : google::LogMessageVoidify() & LOG(INFO) << (LOGGER)->prefix()

// Defines logging levels. While these levels are named with specific objects, the
// levels define the granularity associated with the object. That is, BATCH does
// mean it has to be related to row batches, just events that happen at the row
// batch granularity.
// LOGGER should be a pointer to a Logger object.

// Logs that warn something a continuable error has occurred.
#define QUERY_VLOG_WARNING_LEVEL 1
// Logs that happen a fixed number of times per fragment. e.g. depends only on the plan
#define QUERY_VLOG_FRAGMENT_LEVEL 2
// Logs that happen at a frequency based on the number of files.
#define QUERY_VLOG_FILE_LEVEL 3
// Logs that happen at the frequency of a (IO) buffer
#define QUERY_VLOG_BUFFER_LEVEL 4
// Logs that happen at the frequency of a row batch
#define QUERY_VLOG_BATCH_LEVEL 5
// Logs that happen at a frequency of each row.
#define QUERY_VLOG_ROW_LEVEL 6

#define QUERY_VLOG_WARNING(LOGGER) QUERY_VLOG(LOGGER, QUERY_VLOG_WARNING_LEVEL)
#define QUERY_VLOG_FRAGMENT(LOGGER) QUERY_VLOG(LOGGER, QUERY_VLOG_FRAGMENT_LEVEL)
#define QUERY_VLOG_FILE(LOGGER) QUERY_VLOG(LOGGER, QUERY_VLOG_FILE_LEVEL)
#define QUERY_VLOG_BUFFER(LOGGER) QUERY_VLOG(LOGGER, QUERY_VLOG_BUFFER_LEVEL)
#define QUERY_VLOG_BATCH(LOGGER) QUERY_VLOG(LOGGER, QUERY_VLOG_BATCH_LEVEL)
#define QUERY_VLOG_ROW(LOGGER) \
  if ((LOGGER)->should_log_row()) \
    google::LogMessage( \
        __FILE__, __LINE__, google::GLOG_INFO, (LOGGER)->rows_logged(),\
        &google::LogMessage::SendToLog).stream() << (LOGGER)->prefix()

namespace impala {

// Logger object with prefix and severity. Thread-safe.
class Logger {
 public:
  // Creates a logger with prefix for level. All logs at or below this level
  // are logged, prepended with prefix.
  Logger(const std::string& prefix, int level)
    : prefix_(prefix),
      max_level_(level),
      rows_logged_(0) {
  }

  const std::string& prefix() const { return prefix_; }

  // Returns true if logs at this level should be enabled.
  const bool Enabled(int level) const { return level <= max_level_; }

  // The maximum number of per-row log messages.
  // TODO: make this configuration?
  const int max_rows() const { return 1024; }
  const int rows_logged() const { return rows_logged_; }

  bool should_log_row() const {
    if (!Enabled(QUERY_VLOG_ROW_LEVEL) || rows_logged_ > max_rows()) return false;
    ++rows_logged_;
    return true;
  }

  // Returns a logger that never logs.
  static const Logger* NullLogger();

 private:
  const std::string prefix_;
  const int max_level_;
  mutable int rows_logged_;
};

}

#endif
