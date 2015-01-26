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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;

import com.cloudera.impala.util.ImpalaJdbcClient;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Reads one or more columns from a table via JDBC and returns results as
 * as a tab-delimited string.
 * Applies a query hint to filter results to only the specified input split.
 */
public class JdbcRecordReader implements RecordReader<LongWritable, Text> {
  public static final Log LOG = LogFactory.getLog(JdbcRecordReader.class);

  private Connection con_;
  private ImpalaJdbcClient client_;
  private ResultSet resultSet_;
  private int columnCount_ = 0;
  List<String> colVals_;

  private final String dbName_;
  private final String tblName_;
  private final String colNames_;
  private FileSplit fileSplit_;
  private long rowPos_;

  public JdbcRecordReader(String dbName, String tblName, String colNames) {
    dbName_ = dbName;
    tblName_ = tblName;
    colNames_ = colNames;
    colVals_ = Lists.newArrayList();
    rowPos_ = 0;
  }

  public void initialize(InputSplit split) throws IOException {
    Preconditions.checkState(split instanceof FileSplit);
    fileSplit_ = (FileSplit) split;
    client_ = ImpalaJdbcClient.createClientUsingHiveJdbcDriver();
    rowPos_ = fileSplit_.getStart();
    try {
      client_.connect();
      con_ = client_.getConnection();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      con_.close();
    } catch (SQLException e) {
      LOG.error("Error closing connection: " + e.toString());
    }
  }

  private void execQuery() {
    Preconditions.checkNotNull(fileSplit_);
    // Build query and apply the input split filter.
    StringBuilder sb = new StringBuilder("SELECT  /* +__input_split__=");
    sb.append(String.format("%s@%d@%d */", fileSplit_.getPath().toString(),
        fileSplit_.getStart(),
        fileSplit_.getLength()));
    sb.append(String.format(" %s FROM %s.%s", colNames_, dbName_, tblName_));
    String query = sb.toString();

    try {
      resultSet_ = client_.execQuery(query);
      columnCount_ = resultSet_.getMetaData().getColumnCount();
    } catch (SQLException e) {
      throw new RuntimeException("Error issuing query: '" + query + "': ", e);
    }
  }

  /**
   * Gets the next row value and stores it in 'value'. Currently, row is returned as a
   * string with column values tab-separated.
   * Returns true if more results are available, false otherwise.
   * TODO: Return results in a more structured format (eg HCatRecord). Results should
   * also be returned as a batch or rows, rather than a single row at a time.
   */
  @Override
  public boolean next(LongWritable key, Text value) throws IOException {
    Preconditions.checkNotNull(value);

    // Only execute the query the first time the job runs.
    if (resultSet_ == null) execQuery();
    Preconditions.checkNotNull(resultSet_);

    try {
      // All done.
      if (!resultSet_.next()) return false;
    } catch (SQLException e) {
      throw new RuntimeException("Error fetching row from " +
          dbName_ + "." + tblName_, e);
    }

    colVals_.clear();
    try {
      // Column indexing starts at 1.
      for (int i = 1; i <= columnCount_; ++i) {
        colVals_.add(resultSet_.getString(i));
      }
    } catch (SQLException e) {
      throw new RuntimeException("Error getting column: ", e);
    }

    value.set(Joiner.on("\t").join(colVals_));
    ++rowPos_;
    return true;
  }

  @Override
  public float getProgress() throws IOException {
    // TODO: What to put here?
    return 0;
  }

  @Override
  public LongWritable createKey() {
    return new LongWritable();
  }

  @Override
  public Text createValue() {
    return new Text();
  }

  @Override
  public long getPos() throws IOException {
    return rowPos_;
  }
}