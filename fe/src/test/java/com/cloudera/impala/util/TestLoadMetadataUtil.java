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

package com.cloudera.impala.util;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.catalog.HdfsFileFormat;
import com.cloudera.impala.catalog.HdfsPartition.FileDescriptor;
import com.cloudera.impala.util.LoadMetadataUtil.FileBlocksInfo;
import com.cloudera.impala.util.LoadMetadataUtil.FsKey;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 * Test functionality of methods in LoadMetadataUtil.
 */
public class TestLoadMetadataUtil {
  private final static Logger LOG = LoggerFactory.getLogger(TestLoadMetadataUtil.class);
  private final static String HDFS_BASE_PATH = "hdfs://localhost:20500";
  private final static List<Path> testPathList_ = new LinkedList<Path>();
  private static FileSystem fs_;

  @BeforeClass
  public static void beforeClass() throws IllegalArgumentException, IOException {
    fs_ = new Path(HDFS_BASE_PATH).getFileSystem(LoadMetadataUtil.getConf());
  }

  /**
   * Delete all temporary file / dir created in Hdfs during testing.
   */
  @AfterClass
  public static void afterClass() throws IOException {
    for (Path path: testPathList_) {
      fs_.deleteOnExit(path);
    }
  }

  /**
   * Create a new temporary file in Hdfs and return the file Path.
   */
  private Path createFileInHdfs(String suffix) throws IOException {
    Path filePath = new Path(getTempName(suffix));
    fs_.create(filePath);
    testPathList_.add(filePath);
    return filePath;
  }

  /**
   * Create a new temporary dir in Hdfs and return the file Path.
   */
  private Path createDirInHdfs(String suffix) throws IOException {
    Path dirPath = new Path(getTempName(suffix));
    fs_.mkdirs(dirPath);
    testPathList_.add(dirPath);
    return dirPath;
  }

  /**
   * Return a temporary file / dir name.
   */
  private String getTempName(String suffix) {
    return new StringBuilder(HDFS_BASE_PATH + "/tmp").append(System.currentTimeMillis())
        .append(suffix).toString();
  }

  /**
   * Enum MethodName for different APIs.
   */
  private enum MethodName {
    LOAD_FILE_DESCRIPTORS, LOAD_VIA_LOCATED_FILE_STATUS, LOAD_VIA_LIST_STATUS_ITERATOR;
  }

  /**
   * Test loadFileDescriptors for different file path with different HDFS APIs.
   */
  @Test
  public void testLoadFileDescriptors() throws IOException{
    for (MethodName methodName: MethodName.values()) {
      testDirectory(methodName);
      testFileWithoutCache(methodName);
      testFileWithCache(methodName);
    }
  }

  /**
   * Test if it returns an empty list when the filepath is a directory.
   */
  private void testDirectory(MethodName methodName) throws IOException {
    Map<FsKey, FileBlocksInfo> perFsFileBlocks = Maps.newHashMap();
    Map<String, List<FileDescriptor>> fileDescMap = Maps.newHashMap();
    Path dirPath = createDirInHdfs("dir");
    List<FileDescriptor> fileDesclist = null;
    switch(methodName) {
      case LOAD_FILE_DESCRIPTORS:
        fileDesclist = LoadMetadataUtil.loadFileDescriptors(fs_, dirPath, null,
            HdfsFileFormat.TEXT, perFsFileBlocks, false, dirPath.getName(), null,
            fileDescMap);
        break;
      case LOAD_VIA_LOCATED_FILE_STATUS:
        fileDesclist = LoadMetadataUtil.loadViaListLocatedStatus(fs_, dirPath, null,
            HdfsFileFormat.TEXT, perFsFileBlocks, false, dirPath.getName(), null,
            fileDescMap);
        break;
      case LOAD_VIA_LIST_STATUS_ITERATOR:
        fileDesclist = LoadMetadataUtil.loadViaListStatusIterator(fs_, dirPath, null,
            HdfsFileFormat.TEXT, perFsFileBlocks, false, dirPath.getName(), null,
            fileDescMap);
        break;
      default:
        LOG.error("Unsupported enum method name");
        Preconditions.checkState(false);
    }
    for (FsKey key: perFsFileBlocks.keySet()) {
      assertEquals(HDFS_BASE_PATH, key.toString());
    }
    assertEquals(0, fileDesclist.size());
  }

  /**
   * Test if it returns the correct file descriptor when the filepath is a normal file
   * without cache.
   */
  private void testFileWithoutCache(MethodName methodName) throws IOException {
    Map<FsKey, FileBlocksInfo> perFsFileBlocks = Maps.newHashMap();
    Map<String, List<FileDescriptor>> fileDescMap = Maps.newHashMap();

    Path filePath = createFileInHdfs("file");
    List<FileDescriptor> fileDesclist = null;
    switch (methodName) {
      case LOAD_FILE_DESCRIPTORS:
        fileDesclist = LoadMetadataUtil.loadFileDescriptors(fs_, filePath, null,
            HdfsFileFormat.TEXT, perFsFileBlocks, false, filePath.getName(), null,
            fileDescMap);
        break;
      case LOAD_VIA_LOCATED_FILE_STATUS:
        fileDesclist = LoadMetadataUtil.loadViaListLocatedStatus(fs_, filePath, null,
            HdfsFileFormat.TEXT, perFsFileBlocks, false, filePath.getName(), null,
            fileDescMap);
      break;
      case LOAD_VIA_LIST_STATUS_ITERATOR:
        fileDesclist = LoadMetadataUtil.loadViaListStatusIterator(fs_, filePath, null,
            HdfsFileFormat.TEXT, perFsFileBlocks, false, filePath.getName(), null,
            fileDescMap);
        break;
      default:
        LOG.error("Unsupported enum method name");
        Preconditions.checkState(false);
    }

    for (FsKey key: perFsFileBlocks.keySet()) {
      assertEquals(HDFS_BASE_PATH, key.toString());
    }

    FileStatus fileStatus = fs_.getFileStatus(filePath);
    assertEquals(1, fileDesclist.size());
    assertEquals(filePath.getName(), fileDesclist.get(0).getFileName());
    assertEquals(fileStatus.getLen(), fileDesclist.get(0).getFileLength());
    assertEquals(fileStatus.getModificationTime(),
        fileDesclist.get(0).getModificationTime());
  }

  /**
   * Test if it returns the same file descriptor when the filepath is a normal file with
   * cache.
   */
  private void testFileWithCache(MethodName methodName) throws IOException {
    Map<FsKey, FileBlocksInfo> perFsFileBlocks = Maps.newHashMap();
    Map<String, List<FileDescriptor>> fileDescMap = Maps.newHashMap();

    // Create old file description map
    Path cacheFilePath = createFileInHdfs("fileWithCache");
    Map<String, List<FileDescriptor>> oldFileDescMap = Maps.newHashMap();
    List<FileDescriptor> cacheList = new LinkedList<FileDescriptor>();
    FileStatus fileStatus = fs_.getFileStatus(cacheFilePath);
    FileDescriptor fdInCache = new FileDescriptor(cacheFilePath.getName(),
        fileStatus.getLen(), fileStatus.getModificationTime());
    cacheList.add(fdInCache);
    oldFileDescMap.put(fileStatus.getPath().getParent().toString(), cacheList);
    List<FileDescriptor> fileDesclist = null;
    switch (methodName) {
      case LOAD_FILE_DESCRIPTORS:
        fileDesclist = LoadMetadataUtil.loadFileDescriptors(fs_, cacheFilePath,
            oldFileDescMap, HdfsFileFormat.TEXT, perFsFileBlocks, false,
            cacheFilePath.getName(), null, fileDescMap);
        break;
      case LOAD_VIA_LOCATED_FILE_STATUS:
        fileDesclist = LoadMetadataUtil.loadFileDescriptors(fs_, cacheFilePath,
            oldFileDescMap, HdfsFileFormat.TEXT, perFsFileBlocks, false,
            cacheFilePath.getName(), null, fileDescMap);
        break;
      case LOAD_VIA_LIST_STATUS_ITERATOR:
        fileDesclist = LoadMetadataUtil.loadFileDescriptors(fs_, cacheFilePath,
            oldFileDescMap, HdfsFileFormat.TEXT, perFsFileBlocks, false,
            cacheFilePath.getName(), null, fileDescMap);
        break;
      default:
        LOG.error("Unsupported enum method name");
        Preconditions.checkState(false);
    }

    for (FsKey key: perFsFileBlocks.keySet()) {
      assertEquals(HDFS_BASE_PATH, key.toString());
    }
    assertEquals(1, fileDesclist.size());
    assertEquals(fdInCache, fileDesclist.get(0));
  }
}
