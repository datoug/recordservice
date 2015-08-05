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
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.DelegationTokenInformation;
import org.apache.hadoop.security.token.delegation.HiveDelegationTokenSupport;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ZooKeeper token store implementation.
 *
 * Copied from Hive implementation.
 */
public class ZooKeeperTokenStore implements Closeable {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(ZooKeeperTokenStore.class.getName());

  protected static final String ZK_SEQ_FORMAT = "%010d";
  private static final String NODE_KEYS = "/keys";
  private static final String NODE_TOKENS = "/tokens";

  public static final String DELEGATION_TOKEN_STORE_CLS =
      "recordservice.delegation.token.store.class";
  public static final String DELEGATION_TOKEN_STORE_ZK_CONNECT_STR =
      "recordservice.delegation.token.store.zookeeper.connectString";

  public static final String DELEGATION_TOKEN_STORE_ZK_CONNECT_TIMEOUTMILLIS =
      "recordservice.delegation.token.store.zookeeper.connectTimeoutMillis";
  public static final String DELEGATION_TOKEN_STORE_ZK_ZNODE =
      "recordservice.delegation.token.store.zookeeper.znode";
  public static final String DELEGATION_TOKEN_STORE_ZK_ACL =
      "recordservice.delegation.token.store.zookeeper.acl";
  public static final String DELEGATION_TOKEN_STORE_ZK_ZNODE_DEFAULT =
      "/recordservice";

  public static final String RECORD_SERVICE_PRINCIPAL_CONF =
      "recordservice.kerberos.principal";
  public static final String RECORD_SERVICE_KEY_TAB_CONF =
      "recordservice.kerberos.keytab.file";

  private String rootNode = "";
  private volatile CuratorFramework zkSession;
  private String zkConnectString;
  private int connectTimeoutMillis;
  private List<ACL> newNodeAcl = Arrays.asList(new ACL(Perms.ALL, Ids.AUTH_IDS));

  // A map from sequence number to master keys. Used for fast lookup in 'getMasterKey'
  private final Map<Integer, String> cachedMasterKeys_ =
    new ConcurrentHashMap<Integer, String>();

  // The latest sequence number at the moment.
  private volatile int latestSeq_ = -1;

  /**
   * Exception for internal token store errors that typically cannot be handled by the caller.
   * FIXME: this is really bad because as it extends RuntimeException so there is no easy way
   * to tell if functions throw. Fix this.
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
   * ACLProvider permissions will be used in case parent dirs need to be created
   */
  private final ACLProvider aclDefaultProvider =  new ACLProvider() {

    @Override
    public List<ACL> getDefaultAcl() {
      return newNodeAcl;
    }

    @Override
    public List<ACL> getAclForPath(String path) {
      return getDefaultAcl();
    }
  };

  private final String WHEN_ZK_DSTORE_MSG = " when zookeeper based delegation token storage is enabled"
      + "("+ DELEGATION_TOKEN_STORE_CLS + "=" + ZooKeeperTokenStore.class.getName() + ")";

  private Configuration conf;

  /**
   * Default constructor for dynamic instantiation w/ Configurable
   * (ReflectionUtils does not support Configuration constructor injection).
   */
  protected ZooKeeperTokenStore() {
  }

  public void setConf(Configuration conf) {
    if (conf == null) {
      throw new IllegalArgumentException("conf is null");
    }
    this.conf = conf;
  }

  public void init() {
    zkConnectString = conf.get(DELEGATION_TOKEN_STORE_ZK_CONNECT_STR);
    if (zkConnectString == null || zkConnectString.trim().isEmpty()) {
      throw new IllegalArgumentException("Zookeeper connect string has to be specifed through "
          + DELEGATION_TOKEN_STORE_ZK_CONNECT_STR
          + WHEN_ZK_DSTORE_MSG);
    }
    LOGGER.info("Connecting to zookeeper at: " + zkConnectString);
    connectTimeoutMillis =
        conf.getInt(
            DELEGATION_TOKEN_STORE_ZK_CONNECT_TIMEOUTMILLIS,
            CuratorFrameworkFactory.builder().getConnectionTimeoutMs());
    String aclStr = conf.get(DELEGATION_TOKEN_STORE_ZK_ACL, null);
    if (StringUtils.isNotBlank(aclStr)) {
      this.newNodeAcl = parseACLs(aclStr);
    }
    LOGGER.info("Zookeeper acl: " + aclStr);
    rootNode =
        conf.get(DELEGATION_TOKEN_STORE_ZK_ZNODE,
            DELEGATION_TOKEN_STORE_ZK_ZNODE_DEFAULT);

    try {
      // Install the JAAS Configuration for the runtime
      setupJAASConfig(conf);
    } catch (IOException e) {
      throw new TokenStoreException("Error setting up JAAS configuration for zookeeper client "
          + e.getMessage(), e);
    }
    initClientAndPaths();
  }

  /**
   * Create a path if it does not already exist ("mkdir -p")
   * @param path string with '/' separator
   * @param acl list of ACL entries
   * @throws TokenStoreException
   */
  private void ensurePath(String path, List<ACL> acl)
      throws TokenStoreException {
    try {
      CuratorFramework zk = getSession();
      String node = zk.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
          .withACL(acl).forPath(path);
      LOGGER.info("Created path: {} ", node);
    } catch (KeeperException.NodeExistsException e) {
      // node already exists
      LOGGER.debug("ZNode already exists: {} ", path);
    } catch (Exception e) {
      throw new TokenStoreException("Error creating path " + path, e);
    }
  }

  /**
   * Parse ACL permission string, from ZooKeeperMain private method
   * @param permString
   * @return
   */
  public static int getPermFromString(String permString) {
      int perm = 0;
      for (int i = 0; i < permString.length(); i++) {
          switch (permString.charAt(i)) {
          case 'r':
              perm |= ZooDefs.Perms.READ;
              break;
          case 'w':
              perm |= ZooDefs.Perms.WRITE;
              break;
          case 'c':
              perm |= ZooDefs.Perms.CREATE;
              break;
          case 'd':
              perm |= ZooDefs.Perms.DELETE;
              break;
          case 'a':
              perm |= ZooDefs.Perms.ADMIN;
              break;
          default:
              LOGGER.error("Unknown perm type: " + permString.charAt(i));
          }
      }
      return perm;
  }

  public static String encodeWritable(Writable key) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    key.write(dos);
    dos.flush();
    return Base64.encodeBase64URLSafeString(bos.toByteArray());
  }

  public static void decodeWritable(Writable w, String idStr) throws IOException {
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(Base64.decodeBase64(idStr)));
    w.readFields(in);
  }

  /**
   * Parse comma separated list of ACL entries to secure generated nodes, e.g.
   * <code>sasl:recordservice/host1@MY.DOMAIN:cdrwa,sasl:recordservice/host2@MY.DOMAIN:cdrwa</code>
   * @param aclString
   * @return ACL list
   */
  public static List<ACL> parseACLs(String aclString) {
    String[] aclComps = StringUtils.splitByWholeSeparator(aclString, ",");
    List<ACL> acl = new ArrayList<ACL>(aclComps.length);
    for (String a : aclComps) {
      if (StringUtils.isBlank(a)) {
         continue;
      }
      a = a.trim();
      // from ZooKeeperMain private method
      int firstColon = a.indexOf(':');
      int lastColon = a.lastIndexOf(':');
      if (firstColon == -1 || lastColon == -1 || firstColon == lastColon) {
         LOGGER.error(a + " does not have the form scheme:id:perm");
         continue;
      }
      ACL newAcl = new ACL();
      newAcl.setId(new Id(a.substring(0, firstColon), a.substring(
          firstColon + 1, lastColon)));
      newAcl.setPerms(getPermFromString(a.substring(lastColon + 1)));
      acl.add(newAcl);
    }
    return acl;
  }

  public int addMasterKey(String s) {
    String keysPath = rootNode + NODE_KEYS + "/";
    CuratorFramework zk = getSession();
    String newNode;
    try {
      newNode = zk.create().withMode(CreateMode.PERSISTENT_SEQUENTIAL).withACL(newNodeAcl)
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

  public void updateMasterKey(int keySeq, String s) {
    CuratorFramework zk = getSession();
    String keyPath = rootNode + NODE_KEYS + "/" + String.format(ZK_SEQ_FORMAT, keySeq);
    try {
      zk.setData().forPath(keyPath, s.getBytes());
      cachedMasterKeys_.put(keySeq, s);
    } catch (Exception e) {
      throw new TokenStoreException("Error setting data in " + keyPath, e);
    }
  }

  public boolean removeMasterKey(int keySeq) {
    String keyPath = rootNode + NODE_KEYS + "/" + String.format(ZK_SEQ_FORMAT, keySeq);
    zkDelete(keyPath);
    cachedMasterKeys_.remove(keySeq);
    if (keySeq == latestSeq_) latestSeq_ = -1;
    return true;
  }

  public String[] getMasterKeys() {
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

  public Pair<Integer, String> getMasterKey(int keySeq) {
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

        String keyPath =
            rootNode + NODE_KEYS + "/" + String.format(ZK_SEQ_FORMAT, keySeq);
        byte[] data = zkGetData(keyPath);
        cachedMasterKeys_.put(keySeq, new String(data));
      }

      return Pair.of(keySeq, cachedMasterKeys_.get(keySeq));
    } catch (Exception e) {
      throw new TokenStoreException(
          "Error getting master key for sequence number " + keySeq, e);
    }
  }

  public boolean addToken(DelegationTokenIdentifier tokenIdentifier,
      DelegationTokenInformation token) {
    byte[] tokenBytes = HiveDelegationTokenSupport.encodeDelegationTokenInformation(token);
    String tokenPath = getTokenPath(tokenIdentifier);
    CuratorFramework zk = getSession();
    String newNode;
    try {
      newNode = zk.create().withMode(CreateMode.PERSISTENT).withACL(newNodeAcl)
          .forPath(tokenPath, tokenBytes);
    } catch (Exception e) {
      throw new TokenStoreException("Error creating new node with path " + tokenPath, e);
    }

    LOGGER.info("Added token: {}", newNode);
    return true;
  }

  public boolean removeToken(DelegationTokenIdentifier tokenIdentifier) {
    String tokenPath = getTokenPath(tokenIdentifier);
    zkDelete(tokenPath);
    return true;
  }

  public DelegationTokenInformation getToken(DelegationTokenIdentifier tokenIdentifier) {
    String path = getTokenPath(tokenIdentifier);
    byte[] tokenBytes = zkGetData(path);
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

  public List<DelegationTokenIdentifier> getAllDelegationTokenIdentifiers() {
    String containerNode = rootNode + NODE_TOKENS;
    final List<String> nodes = zkGetChildren(containerNode);
    List<DelegationTokenIdentifier> result = new java.util.ArrayList<DelegationTokenIdentifier>(
        nodes.size());
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

  public void close() throws IOException {
    if (this.zkSession != null) {
      this.zkSession.close();
    }
  }

  private CuratorFramework getSession() {
    if (zkSession == null || zkSession.getState() == CuratorFrameworkState.STOPPED) {
      synchronized (this) {
        if (zkSession == null || zkSession.getState() == CuratorFrameworkState.STOPPED) {
          zkSession =
              CuratorFrameworkFactory.builder().connectString(zkConnectString)
                  .connectionTimeoutMs(connectTimeoutMillis).aclProvider(aclDefaultProvider)
                  .retryPolicy(new ExponentialBackoffRetry(1000, 3)).build();
          zkSession.start();
          cachedMasterKeys_.clear();
          latestSeq_ = -1;
        }
      }
    }
    return zkSession;
  }

  private void setupJAASConfig(Configuration conf) throws IOException {
    if (!UserGroupInformation.getLoginUser().isFromKeytab()) {
      // The process has not logged in using keytab
      // this should be a test mode, can't use keytab to authenticate
      // with zookeeper.
      LOGGER.warn("Login is not from keytab");
      return;
    }
    String principal = getNonEmptyConfVar(conf, RECORD_SERVICE_PRINCIPAL_CONF);
    String keytab = getNonEmptyConfVar(conf, RECORD_SERVICE_KEY_TAB_CONF);
    Utils.setZookeeperClientKerberosJaasConfig(principal, keytab);
  }

  private String getNonEmptyConfVar(Configuration conf, String param) throws IOException {
    String val = conf.get(param);
    if (val == null || val.trim().isEmpty()) {
      throw new IOException("Configuration parameter " + param + " should be set, "
          + WHEN_ZK_DSTORE_MSG);
    }
    return val;
  }


  private void initClientAndPaths() {
    if (this.zkSession != null) {
      this.zkSession.close();
    }
    try {
      ensurePath(rootNode + NODE_KEYS, newNodeAcl);
      ensurePath(rootNode + NODE_TOKENS, newNodeAcl);
    } catch (TokenStoreException e) {
      throw e;
    }
  }

  private String getTokenPath(DelegationTokenIdentifier tokenIdentifier) {
    try {
      return rootNode + NODE_TOKENS + "/" + encodeWritable(tokenIdentifier);
    } catch (IOException ex) {
      throw new TokenStoreException("Failed to encode token identifier", ex);
    }
  }

  private void zkDelete(String path) {
    CuratorFramework zk = getSession();
    try {
      zk.delete().forPath(path);
    } catch (KeeperException.NoNodeException ex) {
      // already deleted
    } catch (Exception e) {
      throw new TokenStoreException("Error deleting " + path, e);
    }
  }

  private List<String> zkGetChildren(String path) {
    CuratorFramework zk = getSession();
    try {
      return zk.getChildren().forPath(path);
    } catch (Exception e) {
      throw new TokenStoreException("Error getting children for " + path, e);
    }
  }

  private byte[] zkGetData(String nodePath) {
    CuratorFramework zk = getSession();
    try {
      return zk.getData().forPath(nodePath);
    } catch (KeeperException.NoNodeException ex) {
      return null;
    } catch (Exception e) {
      throw new TokenStoreException("Error reading " + nodePath, e);
    }
  }

  private int getSeq(String path) {
    String[] pathComps = path.split("/");
    return Integer.parseInt(pathComps[pathComps.length-1]);
  }

  private Map<Integer, byte[]> getAllKeys() throws KeeperException, InterruptedException {
    String masterKeyNode = rootNode + NODE_KEYS;

    // get children of key node
    List<String> nodes = zkGetChildren(masterKeyNode);

    // read each child node, add to results
    Map<Integer, byte[]> result = new HashMap<Integer, byte[]>();
    for (String node : nodes) {
      String nodePath = masterKeyNode + "/" + node;
      byte[] data = zkGetData(nodePath);
      if (data != null) {
        result.put(getSeq(node), data);
      }
    }
    return result;
  }
}
