/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.impala.security;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;

/**
 * A delegation token identifier that is specific to RecordService.
 */
public class DelegationTokenIdentifier
    extends AbstractDelegationTokenIdentifier {
  public static final Text DELEGATION_KIND = new Text("RECORD_SERVICE_DELEGATION_TOKEN");

  /**
   * Create an empty delegation token identifier for reading into.
   */
  public DelegationTokenIdentifier() {
  }

  /**
   * Create a new delegation token identifier
   * @param owner the effective username of the token owner
   * @param renewer the username of the renewer
   * @param realUser the real username of the token owner
   */
  public DelegationTokenIdentifier(Text owner, Text renewer, Text realUser) {
    super(owner, renewer, realUser);
  }

  @Override
  public Text getKind() {
    return DELEGATION_KIND;
  }

  /**
   * Deserializes the identifier from a byte array.
   */
  static DelegationTokenIdentifier deserialize(byte[] str) throws IOException {
    DelegationTokenIdentifier d = new DelegationTokenIdentifier();
    d.readFields(new DataInputStream(new ByteArrayInputStream(str)));
    return d;
  }

  /**
   * Serializes the identifier to a byte array.
   */
  public byte[] serialize() throws IOException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    write(new DataOutputStream(output));
    return output.toByteArray();
  }
}