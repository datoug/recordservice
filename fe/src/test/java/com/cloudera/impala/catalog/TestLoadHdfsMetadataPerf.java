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

package com.cloudera.impala.catalog;
import static com.codahale.metrics.MetricRegistry.name;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.lang3.EnumUtils;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.util.LoadMetadataUtil;
import com.cloudera.impala.util.Metrics;
import com.codahale.metrics.Timer;

/**
 * Test the performance of different HDFS APIs to load block locations in single or
 * multiple threads: listStatus, listLocatedStatus and listStatusIterator.
 * TODO: Add a test method using the functions in the utility classes.
 */
public class TestLoadHdfsMetadataPerf {
  private static final Logger LOG = LoggerFactory
      .getLogger(TestLoadHdfsMetadataPerf.class);
  private static final String DIR_PATH =
      "hdfs://localhost:20500/test-warehouse/tpch.nation";

  // Total available number of concurrent running threads in this pool.
  private static final int THREAD_NUM_PER_REQUEST = 5;

  // Provide a thread pool executor service to receive a callable / runnable object.
  private static ExecutorService executorService_;

  @BeforeClass
  public static void beforeClass() {
    executorService_ = Executors.newFixedThreadPool(THREAD_NUM_PER_REQUEST);
  }

  @AfterClass
  public static void afterClass() {
    executorService_.shutdown();
  }

  @Test
  public void testAll() throws InterruptedException, ExecutionException {
    testMethod(MethodName.LIST_LOCATED_FILE_STATUS, DIR_PATH, 100);
    testMethod(MethodName.LIST_STATUS, DIR_PATH, 100);
    testMethod(MethodName.LIST_STATUS_ITERATOR, DIR_PATH, 100);
  }

  /**
   * Test methods will call this method to list file status. Each thread will be assigned
   * the same number of tasks according to the number of partitions.
   */
  private static void testMethod(MethodName methodName, String dirPath,
      int totalPartitionNum) throws InterruptedException, ExecutionException {
    String name = name(methodName.name(), dirPath);
    Timer.Context timerCtx = Metrics.INSTANCE.getTimerCtx(name);
    List<Future> futureList = new LinkedList<Future>();

    int numUnassignedPartitions = totalPartitionNum;
    int partitionsPerThread = totalPartitionNum / THREAD_NUM_PER_REQUEST ;
    // When there are fewer partitions than threads, assign 1 per thread until there are
    // no partitions left.
    if (partitionsPerThread == 0) {
      partitionsPerThread = 1;
    }

    // Each thread will be assigned with the same number of partitions except when
    // partition number cannot be divisible by THREAD_NUM_PER_REQUEST, or when there are
    // less partition number than the thread number.
    for (int i = 0; i < THREAD_NUM_PER_REQUEST && numUnassignedPartitions > 0; ++i) {
      // When the partition number cannot be divisible by THREAD_NUM_PER_REQUEST, the
      // last thread will be assigned with the partitions left.
      if (i == THREAD_NUM_PER_REQUEST - 1) {
        partitionsPerThread = numUnassignedPartitions;
      }

      futureList.add(executorService_.submit(new FileStatusLoader(methodName,
          dirPath, partitionsPerThread)));

      numUnassignedPartitions -= partitionsPerThread;
    }

    // Block until all futures complete.
    for (Future future: futureList) {
      future.get();
    }
    LOG.info("{} : {}millisec", name, timerCtx.stop() / Metrics.NANOTOMILLISEC);
  }

  /**
   * Enum MethodName for different APIs.
   */
  private enum MethodName {
    LIST_STATUS, LIST_LOCATED_FILE_STATUS, LIST_STATUS_ITERATOR;
  }

  /**
   * Runnable object to list file status.
   */
  private static class FileStatusLoader implements Runnable {
    private final MethodName methodName;
    private final String dirPath;
    private final int partitionNumPerThread;

    public FileStatusLoader(MethodName methodName, String dirPath,
        int partitionNumPerThread) {
      this.methodName = methodName;
      this.dirPath = dirPath;
      this.partitionNumPerThread = partitionNumPerThread;
    }

    public void run() {
      switch (methodName) {
      case LIST_STATUS:
        for (int i = 0; i < partitionNumPerThread; ++i) {
          listStatus(dirPath);
        }
        break;
      case LIST_LOCATED_FILE_STATUS:
        for (int i = 0; i < partitionNumPerThread; ++i) {
          listLocatedStatus(dirPath);
        }
        break;
      case LIST_STATUS_ITERATOR:
        for (int i = 0; i < partitionNumPerThread; ++i) {
          listStatusIterator(dirPath);
        }
      default:
        break;
      }
    }
  }

  /**
   * List file status by calling fileSystem.listStatus.
   */
  private static void listStatus(String dirPath) {
    Path path = new Path(dirPath);
    boolean exceptionThrown = false;
    try {
      FileSystem fs = path.getFileSystem(LoadMetadataUtil.getConf());
      FileStatus[] fileStatus = fs.listStatus(path);
      if (fs.exists(path)) {
        for (FileStatus status: fileStatus) {
          BlockLocation[] locations = fs.getFileBlockLocations(status, 0, status.getLen());
          for (BlockLocation loc: locations) {
            loc.getNames();
            loc.getHosts();
          }
        }
      }
    } catch (IOException e) {
      exceptionThrown = true;
      LOG.error("Failed to list Status", e);
    }
    assertFalse(exceptionThrown);
  }

  /**
   * List file status by calling abstractFileSystem.listStatusIterator.
   */
  private static void listStatusIterator(String dirPath) {
    Path path = new Path(dirPath);
    boolean exceptionThrown = false;
    try {
      AbstractFileSystem fs = AbstractFileSystem.createFileSystem(path.toUri(),
          LoadMetadataUtil.getConf());
      RemoteIterator<FileStatus> iter = fs.listStatusIterator(path);
      while (iter.hasNext()) {
        FileStatus fileStatus = iter.next();
        BlockLocation[] locations = fs.getFileBlockLocations(fileStatus.getPath(), 0,
            fileStatus.getLen());
        for (BlockLocation loc: locations) {
          loc.getNames();
          loc.getHosts();
        }
      }
    } catch (IOException e) {
      exceptionThrown = true;
      LOG.error("Failed to list Status Iterator", e);
    }
    assertFalse(exceptionThrown);
  }

  /**
   * List file status by calling fileSystem.listLocatedStatus.
   */
  private static void listLocatedStatus(String dirPath) {
    Path path = new Path(dirPath);
    boolean exceptionThrown = false;
    try {
      FileSystem fs = path.getFileSystem(LoadMetadataUtil.getConf());
      RemoteIterator<LocatedFileStatus> iterator = fs.listLocatedStatus(path);
      if (fs.exists(path)) {
        while (iterator.hasNext()) {
          LocatedFileStatus fileStatus = iterator.next();
          BlockLocation[] locations = fileStatus.getBlockLocations();
          for (BlockLocation loc: locations) {
            loc.getHosts();
            loc.getNames();
          }
        }
      }
    } catch (IOException e) {
      exceptionThrown = true;
      LOG.error("Failed to list Located Status", e);
    }
    assertFalse(exceptionThrown);
  }

  // TODO: Add into perf benchmarks.
  public static void main(String[] args) {
    if (args.length != 3) {
      LOG.error("Please enter MethodName Dirpath TotalPartitionNum.");
      return;
    } else if (!EnumUtils.isValidEnum(MethodName.class, args[0])) {
      LOG.error("Please enter a valid MethodName.");
      return;
    } else if (!args[2].matches("\\d+")) {
      LOG.error("Please enter a positive interger as TotalPartitioNum.");
      return;
    }

    MethodName methodName = MethodName.valueOf(args[0]);
    String dirPath = args[1];
    int totalPartitionNum = Integer.parseInt(args[2]);

    String msg = "List file status for dirPath:" + dirPath + "with totalPartitionNum:"
        + totalPartitionNum + " via " + methodName.name();

    TestLoadHdfsMetadataPerf.beforeClass();

    try {
      LOG.info("Start to " + msg);
      testMethod(methodName, dirPath, totalPartitionNum);
    } catch (Exception e) {
      LOG.error("Failed to " + msg, e);
    } finally {
      TestLoadHdfsMetadataPerf.afterClass();
    }
  }
}
