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
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.DelegationTokenInformation;
import org.apache.hadoop.security.token.delegation.HiveDelegationTokenSupport;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.service.ZooKeeperSession;

/**
 * ZooKeeper token store implementation.
 */
public class ZooKeeperTokenStore {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(ZooKeeperTokenStore.class.getName());

  protected static final String ZK_SEQ_FORMAT = "%010d";
  private static final String NODE_KEYS = "/keys";
  private static final String NODE_TOKENS = "/tokens";

  public static final String DELEGATION_TOKEN_STORE_CLS =
      "recordservice.delegation.token.store.class";

  private final ZooKeeperSession zkSession_;

  // A map from sequence number to master keys. Used for fast lookup in 'getMasterKey'
  private final Map<Integer, String> cachedMasterKeys_ =
    new ConcurrentHashMap<Integer, String>();

  // The latest sequence number at the moment.
  private volatile int latestSeq_ = -1;

  /**
   * Exception for internal token store errors that typically cannot be handled by
   * the caller.
   * FIXME: this is really bad because as it extends RuntimeException so there is no
   * easy way to tell if functions throw. Fix this.
   */
  public static class TokenStoreException extends RuntimeException {
    private static final long serialVersionUID = 249268338223156938L;

    public TokenStoreException(Throwable cause) {
      super(cause);
    }

    public TokenStoreException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  /**
   * Default constructor for dynamic instantiation w/ Configurable
   * (ReflectionUtils does not support Configuration constructor injection).
   */
  protected ZooKeeperTokenStore(ZooKeeperSession zkSession) throws IOException {
    zkSession_ = zkSession;
    initClientAndPaths();

    zkSession_.addNewSessionCb(new ZooKeeperSession.NewSessionCb() {
      @Override
      public void newSession() throws IOException {
        cachedMasterKeys_.clear();
        latestSeq_ = -1;
      }
    });
  }

  public static String encodeWritable(Writable key) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    key.write(dos);
    dos.flush();
    return Base64.encodeBase64URLSafeString(bos.toByteArray());
  }

  public static void decodeWritable(Writable w, String idStr) throws IOException {
    DataInputStream in = new DataInputStream(
        new ByteArrayInputStream(Base64.decodeBase64(idStr)));
    w.readFields(in);
  }

  public int addMasterKey(String s) throws IOException {
    String keysPath = zkSession_.rootZnode() + NODE_KEYS + "/";
    CuratorFramework zk = zkSession_.getSession();
    String newNode;
    try {
      newNode = zk.create()
          .withMode(CreateMode.PERSISTENT_SEQUENTIAL)
          .withACL(zkSession_.newNodeAcl())
          .forPath(keysPath, s.getBytes());
    } catch (Exception e) {
      throw new TokenStoreException("Error creating new node with path " + keysPath, e);
    }
    LOGGER.info("Added key {}", newNode);
    int keySeq = getSeq(newNode);
    cachedMasterKeys_.put(keySeq, s);
    latestSeq_ = keySeq;
    return keySeq;
  }

  public void updateMasterKey(int keySeq, String s) throws IOException {
    CuratorFramework zk = zkSession_.getSession();
    String keyPath = zkSession_.rootZnode() + NODE_KEYS + "/" +
        String.format(ZK_SEQ_FORMAT, keySeq);
    try {
      zk.setData().forPath(keyPath, s.getBytes());
      cachedMasterKeys_.put(keySeq, s);
    } catch (Exception e) {
      throw new TokenStoreException("Error setting data in " + keyPath, e);
    }
  }

  public boolean removeMasterKey(int keySeq) throws IOException {
    String keyPath = zkSession_.rootZnode() + NODE_KEYS + "/" +
        String.format(ZK_SEQ_FORMAT, keySeq);
    zkSession_.zkDelete(keyPath);
    cachedMasterKeys_.remove(keySeq);
    if (keySeq == latestSeq_) latestSeq_ = -1;
    return true;
  }

  public String[] getMasterKeys() throws IOException {
    try {
      Map<Integer, byte[]> allKeys = getAllKeys();
      String[] result = new String[allKeys.size()];
      int resultIdx = 0;
      for (byte[] keyBytes : allKeys.values()) {
        result[resultIdx++] = new String(keyBytes);
      }
      return result;
    } catch (KeeperException ex) {
      throw new TokenStoreException(ex);
    } catch (InterruptedException ex) {
      throw new TokenStoreException(ex);
    }
  }

  public Pair<Integer, String> getMasterKey(int keySeq) throws IOException {
    try {
      if (keySeq < 0) keySeq = latestSeq_;

      if (!cachedMasterKeys_.containsKey(keySeq)) {
        // Can't find the keySeq in the cache. Now we need to find the
        // latest key among all the persisted keys. The keys are added with
        // incrementing sequence numbers, so we just look for the largest
        // sequence number.
        if (keySeq < 0) {
          for (int seq : getAllKeys().keySet()) {
            if (keySeq < seq) keySeq = seq;
          }
          latestSeq_ = keySeq;
        }

        String keyPath = zkSession_.rootZnode() +
            NODE_KEYS + "/" + String.format(ZK_SEQ_FORMAT, keySeq);
        byte[] data = zkSession_.zkGetData(keyPath);
        cachedMasterKeys_.put(keySeq, new String(data));
      }

      return Pair.of(keySeq, cachedMasterKeys_.get(keySeq));
    } catch (Exception e) {
      throw new TokenStoreException(
          "Error getting master key for sequence number " + keySeq, e);
    }
  }

  public boolean addToken(DelegationTokenIdentifier tokenIdentifier,
      DelegationTokenInformation token) throws IOException {
    byte[] tokenBytes =
        HiveDelegationTokenSupport.encodeDelegationTokenInformation(token);
    String tokenPath = getTokenPath(tokenIdentifier);
    CuratorFramework zk = zkSession_.getSession();
    String newNode;
    try {
      newNode = zk.create()
          .withMode(CreateMode.PERSISTENT)
          .withACL(zkSession_.newNodeAcl())
          .forPath(tokenPath, tokenBytes);
    } catch (Exception e) {
      throw new TokenStoreException("Error creating new node with path " + tokenPath, e);
    }

    LOGGER.info("Added token: {}", newNode);
    return true;
  }

  public boolean removeToken(DelegationTokenIdentifier tokenIdentifier)
      throws IOException {
    String tokenPath = getTokenPath(tokenIdentifier);
    zkSession_.zkDelete(tokenPath);
    return true;
  }

  public DelegationTokenInformation getToken(
      DelegationTokenIdentifier tokenIdentifier) throws IOException {
    String path = getTokenPath(tokenIdentifier);
    byte[] tokenBytes = zkSession_.zkGetData(path);
    if (tokenBytes == null) {
      throw new TokenStoreException("Token does not exist in ZK: id=" +
          tokenIdentifier + " path=" + path, null);
    }
    try {
      return HiveDelegationTokenSupport.decodeDelegationTokenInformation(tokenBytes);
    } catch (Exception ex) {
      throw new TokenStoreException("Failed to decode token", ex);
    }
  }

  public List<DelegationTokenIdentifier> getAllDelegationTokenIdentifiers()
      throws IOException {
    String containerNode = zkSession_.rootZnode() + NODE_TOKENS;
    final List<String> nodes = zkSession_.zkGetChildren(containerNode);
    List<DelegationTokenIdentifier> result =
        new ArrayList<DelegationTokenIdentifier>(nodes.size());
    for (String node : nodes) {
      DelegationTokenIdentifier id = new DelegationTokenIdentifier();
      try {
        decodeWritable(id, node);
        result.add(id);
      } catch (Exception e) {
        LOGGER.warn("Failed to decode token '{}'", node);
      }
    }
    return result;
  }

  private void initClientAndPaths() throws IOException {
    zkSession_.ensurePath(zkSession_.rootZnode() + NODE_KEYS, zkSession_.newNodeAcl());
    zkSession_.ensurePath(zkSession_.rootZnode() + NODE_TOKENS, zkSession_.newNodeAcl());
  }

  private String getTokenPath(DelegationTokenIdentifier tokenIdentifier) {
    try {
      return zkSession_.rootZnode() + NODE_TOKENS + "/" +
          encodeWritable(tokenIdentifier);
    } catch (IOException ex) {
      throw new TokenStoreException("Failed to encode token identifier", ex);
    }
  }

  private int getSeq(String path) {
    String[] pathComps = path.split("/");
    return Integer.parseInt(pathComps[pathComps.length-1]);
  }

  private Map<Integer, byte[]> getAllKeys()
      throws KeeperException, InterruptedException, IOException {
    String masterKeyNode = zkSession_.rootZnode() + NODE_KEYS;

    // get children of key node
    List<String> nodes = zkSession_.zkGetChildren(masterKeyNode);

    // read each child node, add to results
    Map<Integer, byte[]> result = new HashMap<Integer, byte[]>();
    for (String node : nodes) {
      String nodePath = masterKeyNode + "/" + node;
      byte[] data = zkSession_.zkGetData(nodePath);
      if (data != null) {
        result.put(getSeq(node), data);
      }
    }
    return result;
  }
}
