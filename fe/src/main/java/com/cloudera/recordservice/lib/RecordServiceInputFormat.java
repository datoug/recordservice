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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * An InputFormat that reads one or more columns from the RecordService.
 */
public class RecordServiceInputFormat extends FileInputFormat<LongWritable, Text> {
  public static final Log LOG = LogFactory.getLog(RecordServiceInputFormat.class);

  public final static String DB_NAME_CONF = "db.name";
  public final static String TBL_NAME_CONF = "table.name";
  public final static String COL_NAMES_CONF = "col.names";

  private String dbName_;
  private String tblName_;
  private String colNames_;

  @Override
  public InputSplit[] getSplits(JobConf jobConf, int numSplits) throws IOException {
    return super.getSplits(jobConf, numSplits);
  }

  private void initialize(Configuration jobConf) {
    dbName_ = jobConf.get(DB_NAME_CONF);
    if (dbName_ == null) {
      throw new IllegalArgumentException(DB_NAME_CONF + " not specified.");
    }
    tblName_ = jobConf.get(TBL_NAME_CONF);
    if (tblName_ == null) {
      throw new IllegalArgumentException(TBL_NAME_CONF + " not specified.");
    }
    colNames_ = jobConf.get(COL_NAMES_CONF);
    if (colNames_ == null) {
      throw new IllegalArgumentException(COL_NAMES_CONF + " not specified.");
    }
    LOG.info(String.format("Db=%s, Tbl=%s, Cols=%s", dbName_, tblName_, colNames_));
  }

  /**
   * Creates a new RecordReader which reads from the RecordService. Columns
   * are returned as a tab-delimited string (Text).
   */
  @Override
  public RecordReader<LongWritable, Text> getRecordReader(InputSplit split,
      JobConf job, Reporter reporter) throws IOException {
    initialize(job);
    JdbcRecordReader reader = new JdbcRecordReader(dbName_, tblName_, colNames_);
    reader.initialize(split);
    return (RecordReader<LongWritable, Text>) reader;
  }
}