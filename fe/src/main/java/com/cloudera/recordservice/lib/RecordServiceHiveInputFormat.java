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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.io.HiveRecordReader;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import com.google.common.base.Joiner;

/**
 * A HiveInputFormat that redirects all HDFS reads to the RecordService. This is
 * done by extracting the Hive query plan and finding the TableScanOperators. From there
 * we know the table being scanned and the columns being projected. A
 * RecordServiceRecordReader is then passed to the HiveRecordReader instead of the
 * table's actual InputFormat (in getRecordReader())
 *
 * This can be enabled to happen transparently by setting this class as the
 * default hive input format. To do this put the following in the hive-site.xml:
 *
 * <property>
 *   <name>hive.input.format</name>
 *   <value>com.cloudera.recordservice.lib.RecordServiceHiveInputFormat</value>
 * </property>
 *
 * There are still some issues with this implementation:
 * - It doesn't handle multiple table scans of different tables.
 * - Hive short-circuits some statements like SELECT * (it just cats the file). This
 *   statement will not go through the RecordService.
 * - Results may be returned to the Hive client as NULL in some cases since our
 *   RecordReader is a hack.
 * - Queries that don't specify any columns and multi-table joins probably don't work.
 */
@SuppressWarnings("rawtypes")
public class RecordServiceHiveInputFormat<K extends WritableComparable,
    V extends Writable> extends HiveInputFormat<K, V> {

  // Set in initTableScanProjection()
  private String dbName_;
  private String tblName_;
  private String colNames_;

  /**
   * Replaces whatever the actual InputFormat/RecordReader is with the RecordService
   * version.
   */
  @Override
  public RecordReader getRecordReader(InputSplit split, JobConf job,
      Reporter reporter) throws IOException {
    // Initialize everything needed to read the projection information.
    HiveInputSplit hsplit = (HiveInputSplit) split;

    boolean nonNative = false;
    if (pathToPartitionInfo == null) init(job);

    PartitionDesc part = pathToPartitionInfo.get(hsplit.getPath().toString());
    if ((part != null) && (part.getTableDesc() != null)) {
      Utilities.copyTableJobPropertiesToConf(part.getTableDesc(), job);
      nonNative = part.getTableDesc().isNonNative();
    }

    initTableScanProjection(job, hsplit.getPath().toString(),
        hsplit.getPath().toUri().getPath(), nonNative);

    // Create a RecordServiceRecordReader and wrap it with a HiveRecordReader
    // to read the data.
    JdbcRecordReader rsRr = new JdbcRecordReader(dbName_, tblName_, colNames_);
    rsRr.initialize(hsplit);

    // Pass the RecordService as the target InputFormat (this will override
    // the actual input format).
    String inputFormatClassName = null;
    Class inputFormatClass = null;
    try {
      inputFormatClassName = RecordServiceInputFormat.class.getName();
      inputFormatClass = job.getClassByName(inputFormatClassName);
    } catch (Exception e) {
      throw new IOException("cannot find class " + inputFormatClassName, e);
    }
    HiveRecordReader<K,V> rr = new HiveRecordReader(rsRr, job);
    rr.initIOContext(hsplit, job, inputFormatClass, rsRr);
    return rr;
  }

  /**
   * Finds a TableScanOperator in the Hive query plan and reads the targeted
   * db name, table name, and projected column name(s). These will later be
   * passed to the RecordService record reader.
   * Most of this code was copied from the Hive internals and HiveInputFormat.
   */
  protected void initTableScanProjection(JobConf jobConf, String splitPath,
      String splitPathWithNoSchema, boolean nonNative) {
    MapWork mrwork = Utilities.getMapWork(jobConf);
    pathToPartitionInfo = mrwork.getPathToPartitionInfo();
    for (PartitionDesc desc: pathToPartitionInfo.values()) {
      String fqTblName[] = desc.getTableName().split("\\.");
      dbName_ = fqTblName[0];
      tblName_ = fqTblName[1];
    }

    if(mrwork.getPathToAliases() == null) return;

    ArrayList<String> aliases = new ArrayList<String>();
    Iterator<Entry<String, ArrayList<String>>> iterator = mrwork
        .getPathToAliases().entrySet().iterator();

    // Most of the logic is copied from the internals of Hive.
    while (iterator.hasNext()) {
      Entry<String, ArrayList<String>> entry = iterator.next();
      String key = entry.getKey();
      boolean match;
      if (nonNative) {
        // For non-native tables, we need to do an exact match to avoid
        // HIVE-1903.  (The table location contains no files, and the string
        // representation of its path does not have a trailing slash.)
        match = splitPath.equals(key) || splitPathWithNoSchema.equals(key);
      } else {
        // But for native tables, we need to do a prefix match for
        // subdirectories.  (Unlike non-native tables, prefix mixups don't seem
        // to be a potential problem here since we are always dealing with the
        // path to something deeper than the table location.)
        match = splitPath.startsWith(key) || splitPathWithNoSchema.startsWith(key);
      }
      if (match) {
        ArrayList<String> list = entry.getValue();
        for (String val : list) {
          aliases.add(val);
        }
      }
    }

    for (String alias : aliases) {
      Operator<? extends OperatorDesc> op = mrwork.getAliasToWork().get(alias);
      if (op instanceof TableScanOperator) {
        // TODO: Handle case where there are multiple TableScanOperators.
        TableScanOperator ts = (TableScanOperator) op;

        // Set the projected column and table info for the RecordServiceRecordReader.
        colNames_ = Joiner.on(",").join(ts.getNeededColumns());
      }
    }
  }
}