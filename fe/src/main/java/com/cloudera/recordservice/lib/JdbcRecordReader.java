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

package com.cloudera.recordservice.lib;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.cloudera.impala.util.ImpalaJdbcClient;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Reads one or more columns from a table via JDBC and returns results as
 * as a tab-delimited string.
 * Applies a query hint to filter results to only the specified input split.
 */
public class JdbcRecordReader extends RecordReader<Void, Text> {
  public static final Log LOG =
      LogFactory.getLog(JdbcRecordReader.class);

  private Connection con_;
  private ImpalaJdbcClient client;
  private ResultSet resultSet_;
  private int columnCount_ = 0;
  List<String> colVals_;
  private Text value_;

  private final String dbName_;
  private final String tblName_;
  private final String colNames_;
  private FileSplit fileSplit_;

  public JdbcRecordReader(String dbName, String tblName,
      String colNames) {
    dbName_ = dbName;
    tblName_ = tblName;
    colNames_ = colNames;
  }

  @Override
  public void close() throws IOException {
    try {
      con_.close();
    } catch (SQLException e) {
      LOG.error("Error closing connection: " + e.toString());
    }
  }

  public boolean next() throws IOException {
    // Only execute the query the first time the job runs.
    if (resultSet_ == null) {
      String query = null;
      try {
        // Apply the input split filter.
        StringBuilder sb = new StringBuilder("SELECT  /* +__input_split__=");
        sb.append(String.format("%s@%d@%d */", fileSplit_.getPath().toString(),
            fileSplit_.getStart(),
            fileSplit_.getLength()));
        sb.append(String.format(" %s FROM %s.%s", colNames_, dbName_, tblName_));
        query = sb.toString();
        resultSet_ = client.execQuery(query);
        columnCount_ = resultSet_.getMetaData().getColumnCount();
        colVals_ = Lists.newArrayListWithCapacity(columnCount_);
      } catch (SQLException e) {
        throw new RuntimeException("Error issuing query: '" + query + "': ", e);
      }
    }

    try {
      // All done.
      if (!resultSet_.next()) return false;
    } catch (SQLException e) {
      throw new RuntimeException("Error fetching row from " +
          dbName_ + "." + tblName_, e);
    }

    Preconditions.checkNotNull(colVals_);
    colVals_.clear();

    try {
      // Column indexing starts at 1.
      for (int i = 1; i <= columnCount_; ++i) {
        colVals_.add(resultSet_.getString(i));
      }
    } catch (SQLException e) {
      throw new RuntimeException("Error getting column: ", e);
    }
    value_.set(Joiner.on("\t").join(colVals_));
    return true;
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    client = ImpalaJdbcClient.createClientUsingHiveJdbcDriver();
    try {
      client.connect();
      con_ = client.getConnection();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    Preconditions.checkState(split instanceof FileSplit);
    fileSplit_ = (FileSplit) split;
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (value_ == null) value_ = new Text();
    return next();
  }

  @Override
  public Void getCurrentKey() throws IOException, InterruptedException {
    return null;
  }

  /**
   * Gets the current row. Returns results as a string, with columns separated
   * by tabs.
   * TODO: Return something that is more structured (ex.  Avro).
   */
  @Override
  public Text getCurrentValue() throws IOException, InterruptedException {
    return value_;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    // TODO: What to put here?
    return 0;
  }
}