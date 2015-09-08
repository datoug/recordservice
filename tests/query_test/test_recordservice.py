#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#
# This test suite validates query results in recordservice are always updated.

from subprocess import call
from tests.common.impala_test_suite import *

TEST_DB1 = 'tmpDb1'
TEST_DB2 = 'tmpDb2'
TEST_TBL1 = 'tmpTbl1'
TEST_TBL2 = 'tmpTbl2'
TEST_TBL3 = 'tmpTbl3'
NOT_EXIST_TBL = 'notExistTbl'
HDFS_LOCATION = '/tmpLocation'
FILE1_PATH = 'testdata/rsHdfsData/file1'
FILE2_PATH = 'testdata/rsHdfsData/file2'

class TestRecordService(ImpalaTestSuite):

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    # Only test text format
    super(TestRecordService, cls).add_test_dimensions()
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'text')

  def setup_method(self, method):
    self.execute_query('drop table if exists %s.%s' % (TEST_DB1, NOT_EXIST_TBL))
    self.execute_query('drop table if exists %s.%s' % (TEST_DB1, TEST_TBL1))
    self.execute_query('drop table if exists %s.%s' % (TEST_DB1, TEST_TBL2))
    self.execute_query('drop table if exists %s.%s' % (TEST_DB2, TEST_TBL1))
    self.execute_query('drop table if exists %s.%s' % (TEST_DB2, TEST_TBL2))
    self.execute_query('drop table if exists %s.%s' % (TEST_DB2, TEST_TBL3))
    call(["hadoop", "fs", "-rm", "-r", HDFS_LOCATION])
    self.execute_query('create database if not exists %s' % TEST_DB1)
    self.execute_query(
      'create table if not exists %s.%s (i integer, s string)' % (TEST_DB1, TEST_TBL1))
    self.execute_query(
      "insert into %s.%s values (1, 'a'),(2, 'b'),(3, 'c')" % (TEST_DB1, TEST_TBL1))
    self.execute_query('create database if not exists %s' % TEST_DB2)
    self.execute_query(
      'create table if not exists %s.%s (s1 string, i integer, s2 string)' %
      (TEST_DB2, TEST_TBL1))
    self.execute_query(
      "insert into %s.%s values ('a', 1, 'r'),('b', 2, 's'),('c', 3, 't')" %
      (TEST_DB2, TEST_TBL1))
    self.execute_query(
      'create table if not exists %s.%s (s string, i integer)' % (TEST_DB2, TEST_TBL2))
    self.execute_query(
      "insert into %s.%s values ('a', 5),('b', -9),('c', 10)" % (TEST_DB2, TEST_TBL2))

  def teardown_method(self, method):
    self.execute_query('drop table if exists %s.%s' % (TEST_DB1, NOT_EXIST_TBL))
    self.execute_query('drop table if exists %s.%s' % (TEST_DB1, TEST_TBL1))
    self.execute_query('drop table if exists %s.%s' % (TEST_DB1, TEST_TBL2))
    self.execute_query('drop database if exists %s' % TEST_DB1)
    self.execute_query('drop table if exists %s.%s' % (TEST_DB2, TEST_TBL1))
    self.execute_query('drop table if exists %s.%s' % (TEST_DB2, TEST_TBL2))
    self.execute_query('drop table if exists %s.%s' % (TEST_DB2, TEST_TBL3))
    self.execute_query('drop database if exists %s' % TEST_DB2)
    call(["hadoop", "fs", "-rm", "-r", HDFS_LOCATION])

  def test_recordservice_query(self, vector):
    # Test basic queries: insert value, alter table, insert table, drop table and etc.
    self.run_test_case('QueryTest/recordservice-basic', vector)

    # Insert table with hdfs path as location
    call(["hadoop", "fs", "-mkdir", HDFS_LOCATION])
    file1 = os.path.join(os.environ['IMPALA_HOME'], FILE1_PATH)
    call(["hadoop", "fs", "-put", file1, HDFS_LOCATION])
    self.execute_query(
      "create external table if not exists %s.%s (i integer) location '%s'" %
      (TEST_DB1, TEST_TBL2, HDFS_LOCATION))
    self.run_test_case('QueryTest/recordservice-hdfs-before', vector)

    # Add new file into hdfs path
    file2 = os.path.join(os.environ['IMPALA_HOME'], FILE2_PATH)
    call(["hadoop", "fs", "-put", file2, HDFS_LOCATION])
    self.run_test_case('QueryTest/recordservice-hdfs-after', vector)

    # select queries that reference multiple tables or the same table more than once
    self.execute_query(
      "create external table %s.%s (i integer) partitioned by (parCol integer)" %
      (TEST_DB2, TEST_TBL3))
    self.run_test_case('QueryTest/recordservice-multiple-tables', vector)

    # select queries on partitioned table
    # add partition
    self.execute_query("alter table %s.%s add partition (parCol=1) location '%s'" %
      (TEST_DB2, TEST_TBL3, HDFS_LOCATION))
    self.execute_query("alter table %s.%s add partition (parCol=2) location '%s'" %
      (TEST_DB2, TEST_TBL3, HDFS_LOCATION))
    self.run_test_case('QueryTest/recordservice-add-partition', vector)

    # drop partition
    self.execute_query("alter table %s.%s drop partition (parCol=2)" %
      (TEST_DB2, TEST_TBL3))
    self.run_test_case('QueryTest/recordservice-drop-partition', vector)