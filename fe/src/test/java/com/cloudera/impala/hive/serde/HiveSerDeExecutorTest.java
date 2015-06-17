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

package com.cloudera.impala.hive.serde;

import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.booleanTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.byteTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.doubleTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.floatTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.intTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.longTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.shortTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.stringTypeInfo;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.OpenCSVSerde;
import org.apache.hadoop.hive.serde2.RegexSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.common.JniUtil;
import com.cloudera.impala.thrift.TColumnData;
import com.cloudera.impala.thrift.TSerDeInit;
import com.cloudera.impala.thrift.TSerDeInput;
import com.cloudera.impala.thrift.TSerDeOutput;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

// Unit tests for HiveSerDeExecutor.
// TODO: cover more serdes, e.g., lazy serdes.
// TODO: test binary when we can handle more serdes (currently RegexSerDe
// doesn't support binary, and OpenCSVSerDe only support string).

// It's hard to test decimal, timestamp here since we don't have corresponding decoding
// mechanisms in the frontend. We'll test these on the backend.
public class HiveSerDeExecutorTest extends TestCase {

  private final static TBinaryProtocol.Factory protocolFactory
    = new TBinaryProtocol.Factory();
  private final static String REGEX_SERDE =
      "org.apache.hadoop.hive.serde2.RegexSerDe";
  private final static String OPENCSV_SERDE =
      "org.apache.hadoop.hive.serde2.OpenCSVSerde";

  private Map<String, String> propertyMap_;
  private Configuration conf_;
  private List<Boolean> materialized_;
  private HiveSerDeExecutor executor_;
  private AbstractSerDe serde_;
  private List<Integer> nextRowIdx_;

  @Override
  public void setUp() {
    propertyMap_ = new HashMap<String, String>();
    conf_ = new Configuration();
    materialized_ = null;
    executor_ = null;
    serde_ = null;
    nextRowIdx_ = new ArrayList<Integer>();
  }

  private void testHiveSerDe(byte[] inputBytes,
      List<List<Object>> expected) throws SerDeException,
      ImpalaException, TException {
    Preconditions.checkArgument(materialized_ != null,
        "materialized_ should not be null");
    Preconditions.checkArgument(executor_ != null, "executor_ should not be null");
    Preconditions.checkArgument(serde_ != null, "serde_ should not be null");

    byte[] outputBytes = executor_.deserialize(inputBytes);
    TSerDeOutput output = new TSerDeOutput();
    JniUtil.deserializeThrift(protocolFactory, output, outputBytes);

    List<TColumnData> columns = output.batch.cols;
    assertEquals("num of rows doesn't match", expected.size(), output.batch.num_rows);

    StructObjectInspector soi = (StructObjectInspector) serde_.getObjectInspector();
    List<StructField> structFieldRefs = new ArrayList<StructField>();
    List<? extends StructField> allStructFieldRefs = soi.getAllStructFieldRefs();
    for (int i = 0; i < allStructFieldRefs.size(); ++i) {
      if (materialized_.get(i)) structFieldRefs.add(allStructFieldRefs.get(i));
    }

    nextRowIdx_.clear();
    for (int i = 0; i < structFieldRefs.size(); ++i) nextRowIdx_.add(0);

    for (int rowIdx = 0; rowIdx < expected.size(); ++rowIdx) {
      List<Object> currExpectedRow = expected.get(rowIdx);
      // Check whether the number of fields match the expected
      assertEquals("number of fields doesn't match, " + currExpectedRow,
          currExpectedRow.size(), columns.size());
      for (int colIdx = 0; colIdx < currExpectedRow.size(); ++colIdx) {
        // TODO: should we also check field type?
        checkSingle(colIdx, rowIdx, columns, structFieldRefs, currExpectedRow);
      }
    }
  }

  private void checkSingle(int colIdx, int rowIdx,
      List<TColumnData> columns, List<StructField> structFieldRefs,
      List<Object> expectedRow) throws SerDeException {
    TColumnData columnData = columns.get(colIdx);
    StructField sf = structFieldRefs.get(colIdx);
    ObjectInspector oi = sf.getFieldObjectInspector();
    Object expected = expectedRow.get(colIdx);

    if (expected == null) {
      assertTrue(columnData.getIs_null().get(rowIdx));
      return;
    }

    rowIdx = nextRowIdx_.get(colIdx);
    nextRowIdx_.set(colIdx, rowIdx + 1);
    TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromObjectInspector(oi);

    Object actual;
    if (typeInfo.equals(booleanTypeInfo))
      actual = columnData.getBool_vals().get(rowIdx);
    else if (typeInfo.equals(byteTypeInfo))
      actual = columnData.getByte_vals().get(rowIdx);
    else if (typeInfo.equals(shortTypeInfo))
      actual = columnData.getShort_vals().get(rowIdx);
    else if (typeInfo.equals(intTypeInfo))
      actual = columnData.getInt_vals().get(rowIdx);
    else if (typeInfo.equals(longTypeInfo))
      actual = columnData.getLong_vals().get(rowIdx);
    else if (typeInfo.equals(floatTypeInfo))
      actual = columnData.getDouble_vals().get(rowIdx);
    else if (typeInfo.equals(doubleTypeInfo))
      actual = columnData.getDouble_vals().get(rowIdx);
    else if (typeInfo.equals(stringTypeInfo))
      actual = columnData.getString_vals().get(rowIdx);
    else {
      assertTrue("Unhandled type: " + typeInfo.getTypeName(), false);
      return;
    }

    assertEquals(expected, actual);
  }

  private HiveSerDeExecutor createSerDeExecutor(String serdeName)
      throws TException, ImpalaException {
    Preconditions.checkArgument(materialized_ != null,
                                "materialized_ should not be null");
    TSerDeInit init_params = new TSerDeInit();
    init_params.setSerde_class_name(serdeName)
      .setSerde_properties(propertyMap_).setIs_materialized(materialized_);
    byte[] initParamBytes = new TSerializer(protocolFactory).serialize(init_params);
    return new HiveSerDeExecutor(initParamBytes);
  }

  private byte[] createSerDeInput(String... rows) throws TException {
    TSerDeInput input = new TSerDeInput();
    String concatenatedRows = Joiner.on('\n').join(rows);
    List<Integer> rowStartOffsets = new ArrayList<Integer>();
    List<Integer> rowEndOffsets = new ArrayList<Integer>();
    int currRowStart = 0;
    for (int i = 0; i < rows.length; ++i) {
      rowStartOffsets.add(currRowStart);
      rowEndOffsets.add(currRowStart + rows[i].length());
      currRowStart += rows[i].length() + 1;
    }
    input.setData(ByteBuffer.wrap(concatenatedRows.getBytes()))
      .setRow_start_offsets(rowStartOffsets)
      .setRow_end_offsets(rowEndOffsets);
    return new TSerializer(protocolFactory).serialize(input);
  }

  // Get new Properties from Map<String, String>. This is needed
  // for initializing serde_.
  private Properties convertMapToProperties() {
    Properties props = new Properties();
    props.putAll(propertyMap_);
    return props;
  }

  @SuppressWarnings({"deprecation", "unchecked"})
  public void testRegex() throws SerDeException, TException, ImpalaException {
    propertyMap_.clear();
    propertyMap_.put(RegexSerDe.INPUT_REGEX,
        "^(false|true)\t(\\w)\t(\\d+)\t(\\d+)\t(\\d+)\t([\\d.]+)\t([\\d.]+)\t(\\w+)");
    propertyMap_.put(RegexSerDe.INPUT_REGEX_CASE_SENSITIVE, "true");
    propertyMap_.put(serdeConstants.LIST_COLUMNS, "v1,v2,v3,v4,v5,v6,v7,v8");
    propertyMap_.put(serdeConstants.LIST_COLUMN_TYPES,
        "boolean:tinyint:smallint:int:bigint:float:double:string:timestamp");
    propertyMap_.put("columns.comments", "\0\0\0\0\0\0\0");
    propertyMap_.put(serdeConstants.SERIALIZATION_NULL_FORMAT, "NULL");

    serde_ = new RegexSerDe();
    serde_.initialize(conf_, convertMapToProperties());

    // 1. Test a 3-row input with full projection. No nulls.
    byte[] input = createSerDeInput(
        "false\t3\t42\t134\t3000000000\t3.1415926\t3.1415926\tabc",
        "true\t4\t28\t479\t4000000000\t2.414\t2.414\tdef",
        "false\t5\t17\t3525\t5000000000\t9.147\t9.147\tABC");

    materialized_ = Arrays.asList(true, true, true, true, true, true, true, true);
    executor_ = createSerDeExecutor(REGEX_SERDE);

    List<Object> list1 = Arrays.asList((Object)false, (byte)3,
        (short)42, 134, 3000000000l, 3.141592502593994, 3.1415926, "abc");
    List<Object> list2 = Arrays.asList((Object)true, (byte)4,
        (short)28, 479, 4000000000l, 2.4140000343322754, 2.414, "def");
    List<Object> list3 = Arrays.asList((Object)false, (byte)5,
        (short)17, 3525, 5000000000l, 9.147000312805176, 9.147, "ABC");

    testHiveSerDe(input, Arrays.asList(list1, list2, list3));

    // 2. Test a 3-row input with projection on a subset of rows. No nulls.
    materialized_ = Arrays.asList(true, false, true, true, false, false, false, false);
    executor_ = createSerDeExecutor(REGEX_SERDE);

    list1 = Arrays.asList((Object)false, (short)42, 134);
    list2 = Arrays.asList((Object)true, (short)28, 479);
    list3 = Arrays.asList((Object)false, (short)17, 3525);

    testHiveSerDe(input, Arrays.asList(list1, list2, list3));

    // 3. Test a 1-row input with the same executor as above.
    input = createSerDeInput(
        "true\t4\t28\t479\t4000000000\t2.414\t2.414\tdef");

    List<Object> list4 = Arrays.asList((Object)true, (short)28, 479);

    testHiveSerDe(input, Arrays.asList(list4));
  }

  @SuppressWarnings({"deprecation", "unchecked"})
  public void testCSV() throws SerDeException, TException, ImpalaException {
    propertyMap_.clear();
    propertyMap_.put(serdeConstants.LIST_COLUMNS, "v1,v2,v3,v4,v5,v6,v7");
    propertyMap_.put(OpenCSVSerde.SEPARATORCHAR, ",");
    propertyMap_.put(OpenCSVSerde.QUOTECHAR, "\'");
    propertyMap_.put(OpenCSVSerde.ESCAPECHAR, "\0");

    serde_ = new OpenCSVSerde();
    serde_.initialize(conf_, convertMapToProperties());

    // 1. Test a 2-row input with full-projection. With nulls
    materialized_ = Arrays.asList(true, true, true, true, true, true, true);
    executor_ = createSerDeExecutor(OPENCSV_SERDE);

    byte[] input = createSerDeInput(
        "false,3,42,134,3000000000",
        "true,4,28,479,4000000000,2.414");

    List<Object> list1 = Arrays.asList((Object)"false",
        "3", "42", "134", "3000000000", null, null);
    List<Object> list2 = Arrays.asList((Object)"true",
        "4", "28", "479", "4000000000", "2.414", null);

    testHiveSerDe(input, Arrays.asList(list1, list2));

    // 2. Test a 2-row input with the same executor as above. With nulls.
    byte[] input2 = createSerDeInput(
        "false,5,17,3525,5000000000",
        "true,6,29,4124,6000000000");

    List<Object> list3 = Arrays.asList((Object)"false",
        "5", "17", "3525", "5000000000", null, null);
    List<Object> list4 = Arrays.asList((Object)"true",
        "6", "29", "4124", "6000000000", null, null);

    testHiveSerDe(input2, Arrays.asList(list3, list4));

    // 3. Test a 1-row input with projection on a subset of columns. No nulls.
    materialized_ = Arrays.asList(true, false, true, true, false, false, false);
    executor_ = createSerDeExecutor(OPENCSV_SERDE);

    list1 = Arrays.asList((Object)"false", "42", "134");
    list2 = Arrays.asList((Object)"true", "28", "479");

    testHiveSerDe(input, Arrays.asList(list1, list2));
  }
}
