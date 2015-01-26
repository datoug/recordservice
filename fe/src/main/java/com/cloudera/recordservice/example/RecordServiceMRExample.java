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


package com.cloudera.recordservice.example;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.cloudera.recordservice.lib.RecordServiceInputFormat;

/**
 * Example MapReduce job that reads a table from the RecordService and outputs
 * the total number of rows.
 */
public class RecordServiceMRExample extends Configured implements Tool {
  public static final Log LOG =
      LogFactory.getLog(RecordServiceMRExample.class);

  private final static IntWritable one = new IntWritable(1);

  public static class Map extends MapReduceBase
      implements Mapper<NullWritable, Text, NullWritable, IntWritable> {
    @Override
    public void map(NullWritable key, Text value,
        OutputCollector<NullWritable, IntWritable> output, Reporter reporter)
        throws IOException {
      output.collect(NullWritable.get(), one);
    }
  }

  public static class Red extends MapReduceBase
      implements Reducer<NullWritable, IntWritable, NullWritable, IntWritable> {
    @Override
    public void reduce(NullWritable key, Iterator<IntWritable> values,
        OutputCollector<NullWritable, IntWritable> output, Reporter reporter)
        throws IOException {
      int count = 0;
      while (values.hasNext()) {
        values.next();
        ++count;
      }
      output.collect(NullWritable.get(), new IntWritable(count));
    }
  }

  public static void main(String[] args) throws Exception {
    try {
      int res = ToolRunner.run(new Configuration(), new RecordServiceMRExample(), args);
      System.exit(res);
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    JobConf jobConf = new JobConf(getConf(), RecordServiceMRExample.class);

    // Command line args - <db_name>, <table_name>, <col_names>
    jobConf.set(RecordServiceInputFormat.DB_NAME_CONF, args[0]);
    jobConf.set(RecordServiceInputFormat.TBL_NAME_CONF, args[1]);
    jobConf.set(RecordServiceInputFormat.COL_NAMES_CONF, args[2]);

    jobConf.setMapperClass(Map.class);
    jobConf.setReducerClass(Red.class);

    jobConf.setInputFormat(RecordServiceInputFormat.class);
    jobConf.setOutputFormat(TextOutputFormat.class);

    // Get the location of the table (TODO: does not work for partitioned tables).
    HiveMetaStoreClient hmsClient;
    try {
      hmsClient = new HiveMetaStoreClient(new HiveConf());
    } catch (MetaException e) {
      throw new RuntimeException("Error connecting to HMS: ", e);
    }
    try {
      String location = hmsClient.getTable(args[0], args[1]).getSd().getLocation();
      FileInputFormat.setInputPaths(jobConf, new Path(location));
    } catch (Exception e) {
      throw new RuntimeException("HMS Error: ", e);
    }
    FileOutputFormat.setOutputPath(jobConf, new Path(args[3]));

    Job job = new Job(jobConf);
    job.setJarByClass(getClass());
    job.setJobName(getClass().getName());
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(IntWritable.class);

    job.waitForCompletion(true);
    return 0;
  }
}