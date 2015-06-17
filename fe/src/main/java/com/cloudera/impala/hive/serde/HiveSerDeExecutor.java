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

import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.binaryTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.booleanTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.byteTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.doubleTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.floatTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.intTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.longTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.shortTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.stringTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.timestampTypeInfo;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.common.ImpalaRuntimeException;
import com.cloudera.impala.common.JniUtil;
import com.cloudera.impala.extdatasource.thrift.TRowBatch;
import com.cloudera.impala.extdatasource.util.SerializationUtils;
import com.cloudera.impala.thrift.TColumnData;
import com.cloudera.impala.thrift.TSerDeInit;
import com.cloudera.impala.thrift.TSerDeInput;
import com.cloudera.impala.thrift.TSerDeOutput;

// Wrapper class to call Java side serde classes to parse the input byte buffer.
// This class works with HdfsHiveSerdeScanner from the backend. Internally it is
// backed by an Hive serde object. To create the object, we need to pass
// in the specific serde class and a set of properties that are necessary for
// initializing the object. For instance, Hive RegexSerDe needs to know 'columns,
// 'columns.types', 'input.regex', and so on.
// TODO: handle lazy serdes
public class HiveSerDeExecutor {
  private static final Logger LOG = Logger.getLogger(HiveSerDeExecutor.class);

  private final static TBinaryProtocol.Factory protocolFactory =
      new TBinaryProtocol.Factory();

  // A Hive serde object that backs this class. This is used to do the actual
  // deserialization of the input rows, and separate all the fields for each row.
  private final AbstractSerDe serde_;

  // Each element in this list, if set to true, indicates that the corresponding
  // column should be materialized. This list should have the same length as
  // the # of StructField returned by the serde object.
  private final List<Boolean> isMaterialized_;

  // Used to store rows from the backend, and passed to the Hive serde object
  // as parameter.
  private final Text text_;

  // Object inspector for an entire row.
  private final StructObjectInspector soi_;

  // List of structs that describe the fields in a row.
  private final List<StructField> structFieldRefs_;

  // 'cols_' stores a list of column data to be send back in the row batch.
  // 'valCols_' stores a list of list of values of a particular type
  // (e.g., bool_vals, int_vals, binary_vals, etc). Each list of values is a
  // reference to the actual list that's being populated in the corresponding
  // column data in 'cols_'.
  // 'isNullCols_' stores a list of list of booleans for each column data
  // in 'cols_'. Each list of booleans is a reference to the 'is_null' field
  // in the corresponding column data in 'cols_'.
  // Both 'valCols_' and 'isNullCols_' should have the same length as 'cols_'.
  private final List<TColumnData> cols_;
  private final List<List<?>> valCols_;
  private final List<List<Boolean>> isNullCols_;

  @SuppressWarnings("deprecation")
  public HiveSerDeExecutor(byte[] thriftParams) throws ImpalaException {
    TSerDeInit request = new TSerDeInit();
    JniUtil.deserializeThrift(protocolFactory, request, thriftParams);

    // These three fields should all be non-null, since they are all required.
    String serdeClassName = request.getSerde_class_name();
    Map<String, String> serdeProperties = request.getSerde_properties();
    isMaterialized_ = request.getIs_materialized();

    try {
      LOG.debug("Loading Hive SerDe class '" + serdeClassName + "'");
      ClassLoader loader = getClass().getClassLoader();
      Class<?> c = Class.forName(serdeClassName, true, loader);
      Class<? extends AbstractSerDe> serdeClass = c.asSubclass(AbstractSerDe.class);
      Constructor<? extends AbstractSerDe> ctor = serdeClass.getConstructor();
      serde_ = ctor.newInstance();

      // Make sure that the serialized class is Text
      if (serde_.getSerializedClass() != Text.class) {
        throw new ImpalaRuntimeException(
          "Expected serialized class to be 'Text', but found '" +
          serde_.getSerializedClass().getName());
      }

      // We'll need to call initialize() before the deserialize() method.
      // TODO: is it correct to just initialize a Configuration like this?
      Configuration conf = new Configuration();
      Properties properties = new Properties();
      properties.putAll(serdeProperties);

      serde_.initialize(conf, properties);

      soi_ = (StructObjectInspector) serde_.getObjectInspector();
      if (isMaterialized_.size() != soi_.getAllStructFieldRefs().size()) {
        throw new ImpalaRuntimeException(
          "Expected getAllStructFieldRefs() from this serde's inspector to return " +
          isMaterialized_.size() + ", but found " + soi_.getAllStructFieldRefs().size());
      }

      text_ = new Text();
      structFieldRefs_ = new ArrayList<StructField>();
      cols_ = new ArrayList<TColumnData>();
      valCols_ = new ArrayList<List<?>>();
      isNullCols_ = new ArrayList<List<Boolean>>();
      Iterator<? extends StructField> sit = soi_.getAllStructFieldRefs().iterator();
      Iterator<Boolean> it = isMaterialized_.iterator();

      // Find out which columns are needed by the backend. Initialize
      // corresponding TColumnData, and set up its corresponding value list.
      // TODO: handle decimal type here.
      while (sit.hasNext()) {
        StructField sf = sit.next();
        if (it.next()) {
          TColumnData columnData = new TColumnData();
          structFieldRefs_.add(sf);
          cols_.add(columnData);
          List<Boolean> blist = new ArrayList<Boolean>();
          isNullCols_.add(blist);
          columnData.setIs_null(blist);
          ObjectInspector oi = sf.getFieldObjectInspector();
          TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromObjectInspector(oi);

          if (typeInfo.equals(booleanTypeInfo)) {
            List<Boolean> list = new ArrayList<Boolean>();
            columnData.setBool_vals(list);
            valCols_.add(list);
          } else if (typeInfo.equals(byteTypeInfo)) {
            List<Byte> list = new ArrayList<Byte>();
            columnData.setByte_vals(list);
            valCols_.add(list);
          } else if (typeInfo.equals(shortTypeInfo)) {
            List<Short> list = new ArrayList<Short>();
            columnData.setShort_vals(list);
            valCols_.add(list);
          } else if (typeInfo.equals(intTypeInfo)) {
            List<Integer> list = new ArrayList<Integer>();
            columnData.setInt_vals(list);
            valCols_.add(list);
          } else if (typeInfo.equals(longTypeInfo)) {
            List<Long> list = new ArrayList<Long>();
            columnData.setLong_vals(list);
            valCols_.add(list);
          } else if (typeInfo.equals(floatTypeInfo) || typeInfo.equals(doubleTypeInfo)) {
            List<Double> list = new ArrayList<Double>();
            columnData.setDouble_vals(list);
            valCols_.add(list);
          } else if (typeInfo.equals(stringTypeInfo)) {
            List<String> list = new ArrayList<String>();
            columnData.setString_vals(list);
            valCols_.add(list);
          } else if (typeInfo.equals(binaryTypeInfo) ||
                     typeInfo.equals(timestampTypeInfo)) {
            List<ByteBuffer> list = new ArrayList<ByteBuffer>();
            columnData.setBinary_vals(list);
            valCols_.add(list);
          } else {
            throw new SerDeException(
              "Unexpected type info: '" + typeInfo.getTypeName() + "'");
          }
        }
      }
    } catch (ClassNotFoundException e) {
      throw new ImpalaRuntimeException("Unable to find class.", e);
    } catch (NoSuchMethodException e) {
      throw new ImpalaRuntimeException("Unable to find method.", e);
    } catch (InstantiationException e) {
      throw new ImpalaRuntimeException("Unable to instantiate the SerDe object.", e);
    } catch (IllegalAccessException e) {
      throw new ImpalaRuntimeException("Unable to instantiate the SerDe object.", e);
    } catch (InvocationTargetException e) {
      throw new ImpalaRuntimeException("Unable to instantiate the SerDe object.", e);
    } catch (SerDeException e) {
      throw new ImpalaRuntimeException("Unable to initialize the SerDe object.", e);
    }
  }

  /**
   * This method calls a specific Hive serde class to deserialize
   * the input rows embedded in the input parameter. The input consists of
   * a byte buffer containing the rows to deserialize, and a list
   * of integers, containing the length for each row. After calling the dererialize
   * method of the serde class, this method packages the results into a TRowBatch, and
   * sends back to the BE class for further processing.
   *
   * @param thriftParams the input to be deserialized
   * @return a serialized byte array containing the TRowBatch
   * @throws ImpalaException thrown when deserializing input failed
   * @throws TException thrown when serializing output failed
   **/
  @SuppressWarnings("unchecked")
  public byte[] deserialize(byte[] thriftParams)
      throws ImpalaException, TException, SerDeException {
    TSerDeInput request = new TSerDeInput();
    TSerDeOutput result = new TSerDeOutput();
    JniUtil.deserializeThrift(protocolFactory, request, thriftParams);

    int rowStart, rowEnd;
    byte[] data = request.getData();
    int numRows = request.getRow_start_offsetsSize();
    for (List<?> l : isNullCols_) l.clear();
    for (List<?> l : valCols_) l.clear();

    for (int i = 0; i < numRows; ++i) {
      rowStart = request.getRow_start_offsets().get(i);
      rowEnd = request.getRow_end_offsets().get(i);
      text_.set(data, rowStart, rowEnd - rowStart);

      try {
        Object row = serde_.deserialize(text_);
        // Iterate over each field in this row and append it to the
        // corresponding column data
        for (int j = 0; j < structFieldRefs_.size(); ++j) {
          StructField sf = structFieldRefs_.get(j);
          Object val = soi_.getStructFieldData(row, sf);
          isNullCols_.get(j).add(val == null);
          if (val == null) continue;
          if (val instanceof Timestamp)
            val = SerializationUtils.encodeTimestamp((Timestamp)val);
          // TODO: use sun.misc.FloatingDecimal to exclude extra bits?
          else if (val instanceof Float) val = ((Float)val).doubleValue();
          ((List<Object>) valCols_.get(j)).add(val);
        }
      } catch (Exception e) {
        throw new ImpalaRuntimeException(
          "Error happened while deserializing the input", e);
      }
    }

    result.setBatch(new TRowBatch().setCols(cols_).setNum_rows(numRows));
    return new TSerializer(protocolFactory).serialize(result);
  }
}
