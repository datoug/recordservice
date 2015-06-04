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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.common.JniUtil;
import com.cloudera.impala.extdatasource.thrift.TRowBatch;
import com.cloudera.impala.thrift.TColumnData;
import com.cloudera.impala.thrift.TSerDeInput;
import com.cloudera.impala.thrift.TSerDeOutput;

// Wrapper class to call Java side serde classes to parse the input byte buffer.
// This class works with HdfsHiveSerdeScanner from the backend.

// TODO: update the thrift protocol and use the actual serde implementation.
// Split the input rows into columns and send them back through the row batch.
public class HiveSerDeExecutor {
  private static final Logger LOG = Logger.getLogger(HiveSerDeExecutor.class);

  private final static TBinaryProtocol.Factory protocolFactory =
    new TBinaryProtocol.Factory();

  public HiveSerDeExecutor() {
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
  public byte[] deserialize(byte[] thriftParams) throws ImpalaException, TException {
    TSerDeInput request = new TSerDeInput();
    TSerDeOutput result = new TSerDeOutput();
    JniUtil.deserializeThrift(protocolFactory, request, thriftParams);

    TColumnData columnData = new TColumnData();
    List<Boolean> isNull = new ArrayList<Boolean>();
    List<String> stringVals = new ArrayList<String>();

    int rowStart = 0;
    byte[] data = request.getData();

    // TODO: input rows may not start from data[0]. Fix this later.
    for (int i = 0; i < request.getLengthsSize(); ++i) {
      int rowLen = request.getLengths().get(i);
      stringVals.add(new String(data, rowStart, rowLen));
      isNull.add(false);
      // extra one for newline char
      rowStart += rowLen + 1;
    }

    columnData.setIs_null(isNull).setString_vals(stringVals);

    List<TColumnData> cols = new ArrayList<TColumnData>();
    cols.add(columnData);

    result.setBatch(new TRowBatch().setCols(cols).setNum_rows(request.getLengthsSize()));
    return new TSerializer(protocolFactory).serialize(result);
  }
}
