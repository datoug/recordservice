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
import java.io.IOException;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A RecordService specific delegation token secret manager.
 * The secret manager is responsible for generating and accepting the password
 * for each token.
 */
public class DelegationTokenSecretManager
    extends AbstractDelegationTokenSecretManager<DelegationTokenIdentifier> {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(DelegationTokenSecretManager.class.getName());

  /**
   * Create a secret manager
   * @param delegationKeyUpdateInterval the number of seconds for rolling new
   *        secret keys.
   * @param delegationTokenMaxLifetime the maximum lifetime of the delegation
   *        tokens
   * @param delegationTokenRenewInterval how often the tokens must be renewed
   * @param delegationTokenRemoverScanInterval how often the tokens are scanned
   *        for expired tokens
   */
  public DelegationTokenSecretManager(long delegationKeyUpdateInterval,
                                      long delegationTokenMaxLifetime,
                                      long delegationTokenRenewInterval,
                                      long delegationTokenRemoverScanInterval) {
    super(delegationKeyUpdateInterval, delegationTokenMaxLifetime,
          delegationTokenRenewInterval, delegationTokenRemoverScanInterval);
  }

  @Override
  public DelegationTokenIdentifier createIdentifier() {
    return new DelegationTokenIdentifier();
  }

  public synchronized void cancelDelegationToken(String user, String tokenStrForm)
      throws IOException {
    Token<DelegationTokenIdentifier> t = tokenFromString(tokenStrForm);
    cancelToken(t, user);
  }

  public synchronized long renewDelegationToken(String user, String tokenStrForm)
      throws IOException {
    Token<DelegationTokenIdentifier> t = tokenFromString(tokenStrForm);
    return renewToken(t, user);
  }

  public synchronized DelegationTokenManager.DelegationToken getDelegationToken(
      String owner, String renewer, String realUser) throws IOException {
    if (realUser == null) realUser = owner;
    DelegationTokenIdentifier ident = new DelegationTokenIdentifier(
        new Text(owner), new Text(renewer), new Text(realUser));
    Token<DelegationTokenIdentifier> t = new Token<DelegationTokenIdentifier>(
        ident, this);
    return new DelegationTokenManager.DelegationToken(
        encodeIdentifier(ident.serialize()),
        encodePassword(t.getPassword()), t.encodeToUrlString().getBytes());
  }

  public synchronized String getDelegationToken(String renewer) throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    Text owner = new Text(ugi.getUserName());
    Text realUser = null;
    if (ugi.getRealUser() != null) {
      realUser = new Text(ugi.getRealUser().getUserName());
    }
    DelegationTokenIdentifier ident =
        new DelegationTokenIdentifier(owner, new Text(renewer), realUser);
    Token<DelegationTokenIdentifier> t = new Token<DelegationTokenIdentifier>(
        ident, this);
    LOGGER.info("Generated delegation token. Identifer=" + ident);
    return t.encodeToUrlString();
  }

  public String getUserFromToken(String tokenStr) throws IOException {
    DelegationTokenIdentifier id = tokenIdentifierFromTokenString(tokenStr);
    return id.getUser().getShortUserName();
  }

  public Token<DelegationTokenIdentifier> tokenFromString(String tokenStr)
      throws IOException {
    Token<DelegationTokenIdentifier> token = new Token<DelegationTokenIdentifier>();
    try {
      token.decodeFromUrlString(tokenStr);
    } catch (IOException e) {
      throw new InvalidToken("Token is corrupt.");
    }
    return token;
  }

  public DelegationTokenIdentifier tokenIdentifierFromTokenString(String tokenStr)
      throws IOException {
    Token<DelegationTokenIdentifier> token = tokenFromString(tokenStr);
    DelegationTokenIdentifier d = new DelegationTokenIdentifier();
    d.readFields(new DataInputStream(new ByteArrayInputStream(
        token.getIdentifier())));
    return d;
  }

  static String encodeIdentifier(byte[] identifier) {
    return new String(Base64.encodeBase64(identifier));
  }

  static String encodePassword(byte[] password) {
    return new String(Base64.encodeBase64(password));
  }
}
