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

package org.apache.hadoop.hetu.client.rpc;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hetu.client.DatabaseArgs;
import org.apache.hadoop.hetu.client.OzoneDatabase;
import org.apache.hadoop.hetu.client.OzonePartition;
import org.apache.hadoop.hetu.client.OzoneTable;
import org.apache.hadoop.hetu.client.OzoneTablet;
import org.apache.hadoop.hetu.client.OzoneTabletDetails;
import org.apache.hadoop.hetu.client.OzoneTabletLocation;
import org.apache.hadoop.hetu.client.PartitionArgs;
import org.apache.hadoop.hetu.client.TableArgs;
import org.apache.hadoop.hetu.client.io.HetuInputStream;
import org.apache.hadoop.hetu.client.io.HetuOutputStream;
import org.apache.hadoop.hetu.client.io.TabletInputStream;
import org.apache.hadoop.hetu.client.io.TabletOutputStream;
import org.apache.hadoop.hetu.client.protocol.ClientProtocol;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.apache.hadoop.ozone.client.io.LengthInputStream;
import org.apache.hadoop.hetu.hm.helpers.OmDatabaseArgs;
import org.apache.hadoop.hetu.photon.meta.common.StorageEngine;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmDeleteTablets;
import org.apache.hadoop.ozone.om.helpers.OmPartitionArgs;
import org.apache.hadoop.ozone.om.helpers.OmPartitionInfo;
import org.apache.hadoop.ozone.om.helpers.OmTableArgs;
import org.apache.hadoop.ozone.om.helpers.OmTableInfo;
import org.apache.hadoop.ozone.om.helpers.OmTabletArgs;
import org.apache.hadoop.ozone.om.helpers.OmTabletInfo;
import org.apache.hadoop.ozone.om.helpers.OmTabletLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OpenTabletSession;
import org.apache.hadoop.ozone.om.helpers.OzoneAclUtil;
import org.apache.hadoop.ozone.om.helpers.OzoneTabletStatus;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmTabletInfo;
import org.apache.hadoop.ozone.om.helpers.ServiceInfo;
import org.apache.hadoop.ozone.om.helpers.ServiceInfoEx;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.protocolPB.OmTransport;
import org.apache.hadoop.ozone.om.protocolPB.OmTransportFactory;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRoleInfo;
import org.apache.hadoop.ozone.security.GDPRSymmetricKey;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.apache.hadoop.ozone.security.acl.OzoneAclConfig;
import org.apache.hadoop.ozone.security.auth.HetuObj;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Time;
import org.apache.ratis.protocol.ClientId;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import java.io.IOException;
import java.net.URI;
import java.security.InvalidKeyException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_KEY_PROVIDER_CACHE_EXPIRY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_KEY_PROVIDER_CACHE_EXPIRY_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConsts.HETU_OLD_QUOTA_DEFAULT;

/**
 * Ozone RPC Client Implementation, it connects to OM, SCM and DataNode
 * to execute client calls. This uses RPC protocol for communication
 * with the servers.
 */
public class RpcClient implements ClientProtocol {

  private static final Logger LOG =
      LoggerFactory.getLogger(RpcClient.class);

  private final ConfigurationSource conf;
  private final OzoneManagerProtocol ozoneManagerClient;
  private final XceiverClientFactory xceiverClientManager;
  private final int chunkSize;
  private final UserGroupInformation ugi;
  private final ACLType userRights;
  private final ACLType groupRights;
  private final long blockSize;
  private final ClientId clientId = ClientId.randomId();
  private final boolean unsafeByteBufferConversion;
  private Text dtService;
  private final boolean topologyAwareReadEnabled;
  private final boolean checkTabletNameEnabled;
  private final OzoneClientConfig clientConfig;
  private final Cache<URI, KeyProvider> keyProviderCache;

  /**
   * Creates RpcClient instance with the given configuration.
   *
   * @param conf        Configuration
   * @param omServiceId OM HA Service ID, set this to null if not HA
   * @throws IOException
   */
  public RpcClient(ConfigurationSource conf, String omServiceId)
      throws IOException {
    Preconditions.checkNotNull(conf);
    this.conf = conf;
    this.ugi = UserGroupInformation.getCurrentUser();
    // Get default acl rights for user and group.
    OzoneAclConfig aclConfig = this.conf.getObject(OzoneAclConfig.class);
    this.userRights = aclConfig.getUserDefaultRights();
    this.groupRights = aclConfig.getGroupDefaultRights();

    this.clientConfig = conf.getObject(OzoneClientConfig.class);

    OmTransport omTransport = createOmTransport(omServiceId);

    this.ozoneManagerClient = TracingUtil.createProxy(
        new OzoneManagerProtocolClientSideTranslatorPB(omTransport,
            clientId.toString()),
        OzoneManagerProtocol.class, conf
    );
    dtService = omTransport.getDelegationTokenService();
    ServiceInfoEx serviceInfoEx = ozoneManagerClient.getServiceInfo();
    List<X509Certificate> x509Certificates = null;
    if (OzoneSecurityUtil.isSecurityEnabled(conf)) {
      String caCertPem = null;
      List<String> caCertPems = null;
      caCertPem = serviceInfoEx.getCaCertificate();
      caCertPems = serviceInfoEx.getCaCertPemList();
      if (caCertPems == null || caCertPems.isEmpty()) {
        caCertPems = Collections.singletonList(caCertPem);
      }
      x509Certificates = OzoneSecurityUtil.convertToX509(caCertPems);
    }

    this.xceiverClientManager =
        createXceiverClientFactory(x509Certificates);

    int configuredChunkSize = (int) conf
        .getStorageSize(ScmConfigKeys.OZONE_SCM_CHUNK_SIZE_KEY,
            ScmConfigKeys.OZONE_SCM_CHUNK_SIZE_DEFAULT, StorageUnit.BYTES);
    if (configuredChunkSize > OzoneConsts.OZONE_SCM_CHUNK_MAX_SIZE) {
      LOG.warn("The chunk size ({}) is not allowed to be more than"
              + " the maximum size ({}),"
              + " resetting to the maximum size.",
          configuredChunkSize, OzoneConsts.OZONE_SCM_CHUNK_MAX_SIZE);
      chunkSize = OzoneConsts.OZONE_SCM_CHUNK_MAX_SIZE;
    } else {
      chunkSize = configuredChunkSize;
    }

    blockSize = (long) conf.getStorageSize(OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE,
        OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE_DEFAULT, StorageUnit.BYTES);

    unsafeByteBufferConversion = conf.getBoolean(
        OzoneConfigKeys.OZONE_UNSAFEBYTEOPERATIONS_ENABLED,
        OzoneConfigKeys.OZONE_UNSAFEBYTEOPERATIONS_ENABLED_DEFAULT);

    topologyAwareReadEnabled = conf.getBoolean(
        OzoneConfigKeys.OZONE_NETWORK_TOPOLOGY_AWARE_READ_KEY,
        OzoneConfigKeys.OZONE_NETWORK_TOPOLOGY_AWARE_READ_DEFAULT);
    checkTabletNameEnabled = conf.getBoolean(
        OMConfigKeys.OZONE_OM_TABLET_NAME_CHARACTER_CHECK_ENABLED_KEY,
        OMConfigKeys.OZONE_OM_TABLET_NAME_CHARACTER_CHECK_ENABLED_DEFAULT);

    long keyProviderCacheExpiryMs = conf.getTimeDuration(
        OZONE_CLIENT_KEY_PROVIDER_CACHE_EXPIRY,
        OZONE_CLIENT_KEY_PROVIDER_CACHE_EXPIRY_DEFAULT, TimeUnit.MILLISECONDS);
    keyProviderCache = CacheBuilder.newBuilder()
        .expireAfterAccess(keyProviderCacheExpiryMs, TimeUnit.MILLISECONDS)
        .removalListener(new RemovalListener<URI, KeyProvider>() {
          @Override
          public void onRemoval(
              @Nonnull RemovalNotification<URI, KeyProvider> notification) {
            try {
              assert notification.getValue() != null;
              notification.getValue().close();
            } catch (Throwable t) {
              LOG.error("Error closing KeyProvider with uri [" +
                  notification.getKey() + "]", t);
            }
          }
        }).build();
  }

  @NotNull
  @VisibleForTesting
  protected XceiverClientFactory createXceiverClientFactory(
      List<X509Certificate> x509Certificates) throws IOException {
    return new XceiverClientManager(conf,
        conf.getObject(XceiverClientManager.ScmClientConfig.class),
        x509Certificates);
  }

  @VisibleForTesting
  protected OmTransport createOmTransport(String omServiceId)
      throws IOException {
    return OmTransportFactory.create(conf, ugi, omServiceId);
  }

  @Override
  public List<OMRoleInfo> getOmRoleInfos() throws IOException {

    List<ServiceInfo> serviceList = ozoneManagerClient.getServiceList();
    List<OMRoleInfo> roleInfos = new ArrayList<>();

    for (ServiceInfo serviceInfo : serviceList) {
      if (serviceInfo.getNodeType().equals(HddsProtos.NodeType.OM)) {
        OMRoleInfo omRoleInfo = serviceInfo.getOmRoleInfo();
        if (omRoleInfo != null) {
          roleInfos.add(omRoleInfo);
        }
      }
    }
    return roleInfos;
  }

  @Override
  public void createPartition(String databaseName, String tableName, String partitionName) throws IOException {
    createPartition(databaseName, tableName, partitionName, PartitionArgs.newBuilder().build());
  }

  @Override
  public void createPartition(String databaseName, String tableName, String partitionName, PartitionArgs partitionArgs)
      throws IOException {
    verifyDatabaseName(databaseName);
    verifyTableName(tableName);
    verifyPartitionName(partitionName);
    Preconditions.checkNotNull(partitionArgs);
//    verifyCountsTablet(partitionArgs.getCountsInTablet());

    Boolean isVersionEnabled = partitionArgs.getVersioning() == null ?
            Boolean.FALSE : partitionArgs.getVersioning();
    StorageType storageType = partitionArgs.getStorageType() == null ?
            StorageType.DEFAULT : partitionArgs.getStorageType();

    OmPartitionInfo.Builder builder = OmPartitionInfo.newBuilder();
    builder.setDatabaseName(partitionArgs.getDatabaseName());
    builder.setTableName(partitionArgs.getTableName());
    builder.setPartitionName(partitionArgs.getPartitionName());
    builder.setPartitionValue(partitionArgs.getPartitionValue());
    builder.setIsVersionEnabled(isVersionEnabled);
    builder.setStorageType(storageType);
    builder.setSizeInBytes(partitionArgs.getSizeInBytes());
    builder.addAllMetadata(partitionArgs.getMetadata());

    if (partitionArgs.getSizeInBytes() == 0) {
      LOG.info("Creating Partition: {}, with {} as partitionValue.", partitionName, partitionArgs.getPartitionValue());
    }
    ozoneManagerClient.createPartition(builder.build());
  }


  @Override
  public void setPartitionProperty(OmPartitionArgs omPartitionArgs)
          throws IOException {
    Preconditions.checkNotNull(omPartitionArgs);
    ozoneManagerClient.setPartitionProperty(omPartitionArgs);
  }

  @Override
  public OzonePartition getPartitionDetails(String databaseName, String tableName,
          String partitionName)
          throws IOException {
    verifyDatabaseName(databaseName);
    verifyTableName(tableName);
    verifyPartitionName(partitionName);
    OmPartitionInfo partitionInfo = ozoneManagerClient.getPartitionInfo(databaseName, tableName, partitionName);

    return new OzonePartition(
            conf,
            this,
            partitionInfo.getDatabaseName(),
            partitionInfo.getTableName(),
            partitionInfo.getPartitionName(),
            partitionInfo.getStorageType(),
            partitionInfo.getIsVersionEnabled(),
            partitionInfo.getCreationTime(),
            partitionInfo.getModificationTime(),
            partitionInfo.getMetadata());
  }

  @Override
  public void deletePartition(String databaseName, String tableName,
                              String partitionName) throws IOException {
    verifyDatabaseName(databaseName);
    verifyTableName(tableName);
    verifyPartitionName(partitionName);
    ozoneManagerClient.deletePartition(databaseName, tableName, partitionName);
  }

  @Override
  public List<OzonePartition> listPartitions(String databaseName, String tableName,
                                       String partitionPrefix, String prevPartition,
                                       int maxListResult)
          throws IOException {
    List<OmPartitionInfo> partitions = ozoneManagerClient.listPartitions(
            databaseName, tableName, partitionPrefix, prevPartition, maxListResult);

    return partitions.stream().map(partition -> new OzonePartition(
            conf,
            this,
            partition.getDatabaseName(),
            partition.getTableName(),
            partition.getPartitionName(),
            partition.getStorageType(),
            partition.getIsVersionEnabled(),
            partition.getCreationTime(),
            partition.getModificationTime(),
            partition.getMetadata()))
            .collect(Collectors.toList());
  }

  @Override
  public void createDatabase(String databaseName, DatabaseArgs databaseArgs) throws IOException {
    verifyDatabaseName(databaseName);
    Preconditions.checkNotNull(databaseArgs);
    verifyCountsQuota(databaseArgs.getQuotaInTable());
    verifySpaceQuota(databaseArgs.getQuotaInBytes());

    String admin = databaseArgs.getAdmin() == null ?
            ugi.getUserName() : databaseArgs.getAdmin();
    String owner = databaseArgs.getOwner() == null ?
            ugi.getUserName() : databaseArgs.getOwner();
    int quotaInTable = databaseArgs.getQuotaInTable();
    long quotaInBytes = databaseArgs.getQuotaInBytes();

    //Group ACLs of the User
    List<String> userGroups = Arrays.asList(UserGroupInformation
            .createRemoteUser(owner).getGroupNames());

    OmDatabaseArgs.Builder builder = OmDatabaseArgs.newBuilder();
    builder.setName(databaseName);
    builder.setAdminName(admin);
    builder.setOwnerName(owner);
    builder.setQuotaInBytes(quotaInBytes);
    builder.setQuotaInTable(quotaInTable);
    builder.setUsedNamespace(0L);
    builder.addAllMetadata(databaseArgs.getMetadata());

    if (databaseArgs.getQuotaInBytes() == 0) {
      LOG.info("Creating Database: {}, with {} as owner.", databaseName, owner);
    } else {
      LOG.info("Creating Database: {}, with {} as owner "
              + "and space quota set to {} bytes, table counts quota set" +
              " to {}", databaseName, owner, quotaInBytes, quotaInTable);
    }
    ozoneManagerClient.createDatabase(builder.build());
  }

  @Override
  public void createDatabase(String databaseName) throws IOException {
    createDatabase(databaseName, DatabaseArgs.newBuilder().build());
  }

  @Override
  public OzoneDatabase getDatabaseDetails(String databaseName)
      throws IOException {
      verifyDatabaseName(databaseName);
      OmDatabaseArgs database = ozoneManagerClient.getDatabaseInfo(databaseName);
      return new OzoneDatabase(
              conf,
              this,
              database.getName(),
              database.getAdminName(),
              database.getOwnerName(),
              database.getQuotaInBytes(),
              database.getQuotaInTable(),
              database.getUsedNamespace(),
              database.getCreationTime(),
              database.getModificationTime(),
              database.getMetadata());
  }

  @Override
  public void deleteDatabase(String databaseName) throws IOException {
    verifyDatabaseName(databaseName);
    ozoneManagerClient.deleteDatabase(databaseName);
  }

  @Override
  public List<OzoneDatabase> listDatabase(String user, String databasePrefix, String prevDatabase, int maxListResult) throws IOException {
    List<OmDatabaseArgs>  databases = ozoneManagerClient.listDatabaseByUser(
            user, databasePrefix, prevDatabase, maxListResult);

    return databases.stream().map(db -> new OzoneDatabase(
            conf,
            this,
            db.getName(),
            db.getAdminName(),
            db.getOwnerName(),
            db.getQuotaInBytes(),
            db.getQuotaInTable(),
            db.getUsedNamespace(),
            db.getCreationTime(),
            db.getModificationTime(),
            db.getMetadata()))
            .collect(Collectors.toList());
  }

  @Override
  public List<OzoneDatabase> listDatabase(String databasePrefix, String prevDatabase, int maxListResult) throws IOException {
    List<OmDatabaseArgs>  databases = ozoneManagerClient.listAllDatabases(
            databasePrefix, prevDatabase, maxListResult);

    return databases.stream().map(db -> new OzoneDatabase(
            conf,
            this,
            db.getName(),
            db.getAdminName(),
            db.getOwnerName(),
            db.getQuotaInBytes(),
            db.getQuotaInTable(),
            db.getUsedNamespace(),
            db.getCreationTime(),
            db.getModificationTime()))
            .collect(Collectors.toList());
  }

  @Override
  public boolean checkDatabaseAccess(String databaseName, OzoneAcl auth) throws IOException {
    // TODO
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public boolean setDatabaseOwner(String databaseName, String owner) throws IOException {
    // TODO
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public void setDatabaseQuota(String databaseName, long quotaInNamespace, long quotaInBytes) throws IOException {
    // TODO
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public void renameDatabase(String fromDatabaseName, String toDatabaseName) {
    // TODO
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public void createTable(String databaseName, String tableName)
      throws IOException {
    // TODO: Set auths of current user.
    createTable(databaseName, tableName,
        TableArgs.newBuilder().build());
  }

  @Override
  public void createTable(
      String databaseName, String tableName, TableArgs tableArgs)
      throws IOException {
    verifyDatabaseName(databaseName);
    verifyTableName(tableName);
    Preconditions.checkNotNull(tableArgs);
    verifyCountsQuota(tableArgs.getQuotaInBucket());
    verifySpaceQuota(tableArgs.getQuotaInBytes());

    Boolean isVersionEnabled = tableArgs.getIsVersionEnabled() == null ?
        Boolean.FALSE : tableArgs.getIsVersionEnabled();
    StorageType storageType = tableArgs.getStorageType() == null ?
        StorageType.DEFAULT : tableArgs.getStorageType();
    StorageEngine storageEngine = tableArgs.getStorageEngine() == null ?
        StorageEngine.LSTORE : tableArgs.getStorageEngine();
    int numReplicas = tableArgs.getNumReplicas() == 0 ?
        3 : tableArgs.getNumReplicas();

    OmTableInfo.Builder builder = OmTableInfo.newBuilder();
    builder.setDatabaseName(databaseName)
        .setTableName(tableName)
        .setSchema(tableArgs.getSchema())
        .setIsVersionEnabled(isVersionEnabled)
        .addAllMetadata(tableArgs.getMetadata())
        .setStorageType(storageType)
        .setStorageEngine(storageEngine)
        .setNumReplicas(numReplicas)
        .setBuckets(tableArgs.getBuckets())
        .setCreationTime(Time.now())
        .setUsedBytes(0L)
        .setUsedBucket(0)
        .setQuotaInBytes(tableArgs.getQuotaInBytes())
        .setQuotaInBucket(tableArgs.getQuotaInBucket());

    LOG.info("Creating Table: {}/{}, with Versioning {} and " +
            "Storage Type set to {} ",
        databaseName, tableName, isVersionEnabled, storageType);
    ozoneManagerClient.createTable(builder.build());
  }

  private static void verifyDatabaseName(String databaseName) throws OMException {
    try {
      HddsClientUtils.verifyResourceName(databaseName);
    } catch (IllegalArgumentException e) {
      throw new OMException(e.getMessage(),
              OMException.ResultCodes.INVALID_DATABASE_NAME);
    }
  }

  private static void verifyTableName(String tableName) throws OMException {
    try {
      HddsClientUtils.verifyResourceName(tableName);
    } catch (IllegalArgumentException e) {
      throw new OMException(e.getMessage(),
          OMException.ResultCodes.INVALID_TABLE_NAME);
    }
  }

  private static void verifyPartitionName(String partitionName) throws OMException {
    try {
      HddsClientUtils.verifyResourceName(partitionName);
    } catch (IllegalArgumentException e) {
      throw new OMException(e.getMessage(),
          OMException.ResultCodes.INVALID_PARTITION_NAME);
    }
  }

  private static void verifyCountsQuota(long quota) throws OMException {
    if (quota < OzoneConsts.HETU_QUOTA_RESET || quota == 0) {
      throw new IllegalArgumentException("Invalid values for quota : " +
          "counts quota is :" + quota + ".");
    }
  }

  private static void verifySpaceQuota(long quota) throws OMException {
    if (quota < OzoneConsts.HETU_QUOTA_RESET || quota == 0) {
      throw new IllegalArgumentException("Invalid values for quota : " +
          "space quota is :" + quota + ".");
    }
  }

  /**
   * Helper function to get default acl list for current user.
   *
   * @return listOfAcls
   * */
  private List<OzoneAcl> getAclList() {
    return OzoneAclUtil.getAclList(ugi.getUserName(), ugi.getGroupNames(),
        userRights, groupRights);
  }

  /**
   * Get a valid Delegation Token.
   *
   * @param renewer the designated renewer for the token
   * @return Token<OzoneDelegationTokenSelector>
   * @throws IOException
   */
  @Override
  public Token<OzoneTokenIdentifier> getDelegationToken(Text renewer)
      throws IOException {

    Token<OzoneTokenIdentifier> token =
        ozoneManagerClient.getDelegationToken(renewer);
    if (token != null) {
      token.setService(dtService);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Created token {} for dtService {}", token, dtService);
      }
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Cannot get ozone delegation token for renewer {} to " +
            "access service {}", renewer, dtService);
      }
    }
    return token;
  }

  /**
   * Renew an existing delegation token.
   *
   * @param token delegation token obtained earlier
   * @return the new expiration time
   * @throws IOException
   */
  @Override
  public long renewDelegationToken(Token<OzoneTokenIdentifier> token)
      throws IOException {
    return ozoneManagerClient.renewDelegationToken(token);
  }

  /**
   * Cancel an existing delegation token.
   *
   * @param token delegation token
   * @throws IOException
   */
  @Override
  public void cancelDelegationToken(Token<OzoneTokenIdentifier> token)
      throws IOException {
    ozoneManagerClient.cancelDelegationToken(token);
  }

  @Override
  public void setTableVersioning(
      String databaseName, String tableName, Boolean versioning)
      throws IOException {
    verifyDatabaseName(databaseName);
    verifyTableName(tableName);
    Preconditions.checkNotNull(versioning);
    OmTableArgs.Builder builder = OmTableArgs.newBuilder();
    builder.setDatabaseName(databaseName)
        .setTableName(tableName)
        .setIsVersionEnabled(versioning);
    ozoneManagerClient.setTableProperty(builder.build());
  }

  @Override
  public void setTableStorageType(
      String databaseName, String tableName, StorageType storageType)
      throws IOException {
    verifyDatabaseName(databaseName);
    verifyTableName(tableName);
    Preconditions.checkNotNull(storageType);
    OmTableArgs.Builder builder = OmTableArgs.newBuilder();
    builder.setDatabaseName(databaseName)
        .setTableName(tableName)
        .setStorageType(storageType);
    ozoneManagerClient.setTableProperty(builder.build());
  }

  @Override
  public void setTableQuota(String databaseName, String tableName,
      int quotaInBucket, long quotaInBytes) throws IOException {
    HddsClientUtils.verifyResourceName(databaseName);
    HddsClientUtils.verifyResourceName(tableName);
    verifyCountsQuota(quotaInBucket);
    verifySpaceQuota(quotaInBytes);
    OmTableArgs.Builder builder = OmTableArgs.newBuilder();
    builder.setDatabaseName(databaseName)
        .setTableName(tableName)
        .setQuotaInBytes(quotaInBytes)
        .setQuotaInBucket(quotaInBucket);
    // If the table is old, we need to remind the user on the client side
    // that it is not recommended to enable quota.
    OmTableInfo omTableInfo = ozoneManagerClient.getTableInfo(
        databaseName, tableName);
    if (omTableInfo.getQuotaInBytes() == HETU_OLD_QUOTA_DEFAULT ||
            omTableInfo.getUsedBytes() == HETU_OLD_QUOTA_DEFAULT) {
      LOG.warn("Table {} is created before version 1.1.0, usedBytes or " +
          "usedBucket may be inaccurate and it is not recommended to " +
          "enable quota.", tableName);
    }
    ozoneManagerClient.setTableProperty(builder.build());
  }

  @Override
  public void renameTable(String databaseName, String fromTableName, String toTableName) throws IOException {
    // TODO
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public void renameTables(String databaseName, Map<String, String> tableMap) throws IOException {
    // TODO
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public void deleteTable(
      String databaseName, String tableName) throws IOException {
    verifyDatabaseName(databaseName);
    verifyTableName(tableName);
    ozoneManagerClient.deleteTable(databaseName, tableName);
  }

  @Override
  public OzoneTable getTableDetails(
      String databaseName, String tableName) throws IOException {
    verifyDatabaseName(databaseName);
    verifyTableName(tableName);
    OmTableInfo tableInfo =
        ozoneManagerClient.getTableInfo(databaseName, tableName);
    return new OzoneTable(
        conf,
        this,
        tableInfo.getDatabaseName(),
        tableInfo.getTableName(),
        tableInfo.getSchema(),
        tableInfo.getUsedBytes(),
        tableInfo.getUsedBucket(),
        tableInfo.getCreationTime(),
        tableInfo.getIsVersionEnabled(),
        tableInfo.getMetadata());
  }

  @Override
  public List<OzoneTable> listTables(String databaseName, String tablePrefix,
                                       String prevTable, int maxListResult)
      throws IOException {
    List<OmTableInfo> tables = ozoneManagerClient.listTables(
        databaseName, prevTable, tablePrefix, maxListResult);

    return tables.stream().map(table -> new OzoneTable(
        conf,
        this,
        table.getDatabaseName(),
        table.getTableName(),
        table.getSchema(),
        table.getUsedBytes(),
        table.getQuotaInBucket(),
        table.getCreationTime(),
        table.getIsVersionEnabled(),
        table.getMetadata()))
        .collect(Collectors.toList());
  }

  @Override
  @Deprecated
  public HetuOutputStream createTablet(String databaseName, String tableName, String partitionName,
                                         String tabletName, long size, ReplicationType type, ReplicationFactor factor,
                                         Map<String, String> metadata) throws IOException {

    return createTablet(databaseName, tableName, partitionName, tabletName, size,
        ReplicationConfig.fromTypeAndFactor(type, factor), metadata);
  }

  @Override
  public HetuOutputStream createTablet(
      String databaseName, String tableName, String partitionName,
      String tabletName, long size, ReplicationConfig replicationConfig,
      Map<String, String> metadata)
      throws IOException {
    verifyDatabaseName(databaseName);
    verifyTableName(tableName);
    verifyPartitionName(partitionName);
    if (checkTabletNameEnabled) {
      HddsClientUtils.verifyTabletName(tabletName);
    }
    HddsClientUtils.checkNotNull(tabletName, replicationConfig);
    String requestId = UUID.randomUUID().toString();

    OmTabletArgs.Builder builder = new OmTabletArgs.Builder()
        .setDatabaseName(databaseName)
        .setTableName(tableName)
        .setPartitionName(partitionName)
        .setTabletName(tabletName)
        .setDataSize(size)
        .setType(HddsProtos.ReplicationType
            .valueOf(replicationConfig.getReplicationType().toString()))
        .setFactor(ReplicationConfig.getLegacyFactor(replicationConfig))
        .addAllMetadata(metadata)
        .setAcls(getAclList());

    if (Boolean.parseBoolean(metadata.get(OzoneConsts.GDPR_FLAG))) {
      try{
        GDPRSymmetricKey gKey = new GDPRSymmetricKey(new SecureRandom());
        builder.addAllMetadata(gKey.getKeyDetails());
      } catch (Exception e) {
        if (e instanceof InvalidKeyException &&
            e.getMessage().contains("Illegal key size or default parameters")) {
          LOG.error("Missing Unlimited Strength Policy jars. Please install " +
              "Java Cryptography Extension (JCE) Unlimited Strength " +
              "Jurisdiction Policy Files");
        }
        throw new IOException(e);
      }
    }

    OpenTabletSession openTablet = ozoneManagerClient.openTablet(builder.build());
    return createOutputStream(openTablet, requestId, replicationConfig);
  }

  private KeyProvider.KeyVersion getDEK(FileEncryptionInfo feInfo)
      throws IOException {
    // check crypto protocol version
    OzoneKMSUtil.checkCryptoProtocolVersion(feInfo);
    KeyProvider.KeyVersion decrypted;
    decrypted = OzoneKMSUtil.decryptEncryptedDataEncryptionKey(feInfo,
        getKeyProvider());
    return decrypted;
  }

  @Override
  public HetuInputStream getTablet(
      String databaseName, String tableName, String partitionName,
      String tabletName)
      throws IOException {
    verifyDatabaseName(databaseName);
    verifyTableName(tableName);
    verifyPartitionName(partitionName);

    Preconditions.checkNotNull(tabletName);
    OmTabletArgs tabletArgs = new OmTabletArgs.Builder()
        .setDatabaseName(databaseName)
        .setTableName(tableName)
        .setPartitionName(partitionName)
        .setTabletName(tabletName)
        .setRefreshPipeline(true)
        .setSortDatanodesInPipeline(topologyAwareReadEnabled)
        .build();
    OmTabletInfo tabletInfo = ozoneManagerClient.lookupTablet(tabletArgs);
    return getInputStreamWithRetryFunction(tabletInfo);
  }

  @Override
  public void deleteTablet(
      String databaseName, String tableName, String partitionName,
      String tabletName)
      throws IOException {
    verifyDatabaseName(databaseName);
    verifyTableName(tableName);
    verifyPartitionName(partitionName);
    Preconditions.checkNotNull(tabletName);
    OmTabletArgs tabletArgs = new OmTabletArgs.Builder()
        .setDatabaseName(databaseName)
        .setTableName(tableName)
        .setPartitionName(partitionName)
        .setTabletName(tabletName)
        .build();
    ozoneManagerClient.deleteTablet(tabletArgs);
  }

  @Override
  public void deleteTablets(
          String databaseName, String tableName, String partitionName,
          List<String> tabletNameList)
          throws IOException {
    HddsClientUtils.verifyResourceName(databaseName, tableName, partitionName);
    Preconditions.checkNotNull(tabletNameList);
    OmDeleteTablets omDeleteTablets = new OmDeleteTablets(databaseName, tableName,
        partitionName, tabletNameList);
    ozoneManagerClient.deleteTablets(omDeleteTablets);
  }


  @Override
  public List<OzoneTablet> listTablets(String databaseName, String tableName,
                                       String partitionName, String tabletPrefix, String prevTablet,
                                       int maxListResult)
      throws IOException {
    List<OmTabletInfo> tablets = ozoneManagerClient.listTablets(
      databaseName, tableName, partitionName, prevTablet, tabletPrefix, maxListResult);

    return tablets.stream().map(tablet -> new OzoneTablet(
        tablet.getDatabaseName(),
        tablet.getTableName(),
        tablet.getPartitionName(),
        tablet.getTabletName(),
        tablet.getDataSize(),
        tablet.getCreationTime(),
        tablet.getModificationTime(),
        ReplicationType.valueOf(tablet.getType().toString()),
        tablet.getFactor().getNumber()))
        .collect(Collectors.toList());
  }

  @Override
  public List<RepeatedOmTabletInfo> listTrash(String databaseName, String tableName,
      String partitionName, String startTabletName, String tabletPrefix, int maxKeys) throws IOException {
      Preconditions.checkNotNull(databaseName);
      Preconditions.checkNotNull(tableName);
      Preconditions.checkNotNull(partitionName);
//
//    return ozoneManagerClient.listTrash(databaseName, tableName, partitionName, startTabletName,
//        tabletPrefix, maxKeys);
    // TODO: list trash
      return null;
  }

  @Override
  public boolean recoverTrash(String databaseName, String tableName,
      String partitionName, String tabletName, String destinationPartition) throws IOException {

//    return ozoneManagerClient.recoverTrash(databaseName, tableName, partitionName, tabletName,
//        destinationPartition);
    // TODO: recover trash
    return false;
  }

  @Override
  public OzoneTabletDetails getTabletDetails(
      String databaseName, String tableName, String partitionName,
      String tabletName)
      throws IOException {
    Preconditions.checkNotNull(databaseName);
    Preconditions.checkNotNull(tableName);
    Preconditions.checkNotNull(partitionName);
    Preconditions.checkNotNull(tabletName);
    OmTabletArgs tabletArgs = new OmTabletArgs.Builder()
        .setDatabaseName(databaseName)
        .setTableName(tableName)
        .setPartitionName(partitionName)
        .setTabletName(tabletName)
        .setRefreshPipeline(true)
        .setSortDatanodesInPipeline(topologyAwareReadEnabled)
        .build();
    OmTabletInfo tabletInfo = ozoneManagerClient.lookupTablet(tabletArgs);

    List<OzoneTabletLocation> ozoneTabletLocations = new ArrayList<>();
    long lastTabletOffset = 0L;
    List<OmTabletLocationInfo> omTabletLocationInfos = tabletInfo
        .getLatestVersionLocations().getBlocksLatestVersionOnly();
    for (OmTabletLocationInfo info: omTabletLocationInfos) {
      ozoneTabletLocations.add(new OzoneTabletLocation(info.getContainerID(),
          info.getLocalID(), info.getLength(), info.getOffset(),
          lastTabletOffset));
      lastTabletOffset += info.getLength();
    }
    return new OzoneTabletDetails(tabletInfo.getDatabaseName(), tabletInfo.getTableName(),
        tabletInfo.getPartitionName(), tabletInfo.getTabletName(), tabletInfo.getDataSize(),
        tabletInfo.getCreationTime(), tabletInfo.getModificationTime(), ozoneTabletLocations,
        ReplicationType.valueOf(tabletInfo.getType().toString()), tabletInfo.getMetadata(),
        tabletInfo.getFactor().getNumber());
  }

  @Override
  public void close() throws IOException {
    IOUtils.cleanupWithLogger(LOG, ozoneManagerClient, xceiverClientManager);
    keyProviderCache.invalidateAll();
    keyProviderCache.cleanUp();
  }

  @Override
  public OzoneTabletStatus getOzoneTabletStatus(String databaseName,
         String tableName, String partitionName, String tabletName) throws IOException {
    OmTabletArgs tabletArgs = new OmTabletArgs.Builder()
        .setDatabaseName(databaseName)
        .setTableName(tableName)
        .setPartitionName(partitionName)
        .setTabletName(tabletName)
        .setRefreshPipeline(true)
        .setSortDatanodesInPipeline(topologyAwareReadEnabled)
        .build();
    return ozoneManagerClient.getTabletStatus(tabletArgs);
  }

  @Override
  public HetuInputStream readTablet(String databaseName, String tableName,
      String partitionName, String tabletName) throws IOException {
    OmTabletArgs tabletArgs = new OmTabletArgs.Builder()
        .setDatabaseName(databaseName)
        .setTableName(tableName)
        .setPartitionName(partitionName)
        .setTabletName(tabletName)
        .setSortDatanodesInPipeline(topologyAwareReadEnabled)
        .build();
    OmTabletInfo tabletInfo = ozoneManagerClient.lookupTablet(tabletArgs);
    return getInputStreamWithRetryFunction(tabletInfo);
  }

  @Override
  public HetuOutputStream createTablet(String databaseName, String tableName,
      String tabletName, String partitionName, long size, ReplicationType type, ReplicationFactor factor,
      boolean overWrite, boolean recursive) throws IOException {
    return createTablet(databaseName, tableName, partitionName, tabletName, size,
        ReplicationConfig.fromTypeAndFactor(type, factor), overWrite,
        recursive);
  }

  /**
   * Create InputStream with Retry function to refresh pipeline information
   * if reads fail.
   *
   * @param tabletInfo
   * @return
   * @throws IOException
   */
  private HetuInputStream getInputStreamWithRetryFunction(
      OmTabletInfo tabletInfo) throws IOException {
    return createInputStream(tabletInfo, omTabletInfo -> {
      try {
        OmTabletArgs omTabletArgs = new OmTabletArgs.Builder()
            .setDatabaseName(omTabletInfo.getDatabaseName())
            .setTableName(omTabletInfo.getTableName())
            .setPartitionName(omTabletInfo.getPartitionName())
            .setTabletName(omTabletInfo.getTabletName())
            .setRefreshPipeline(true)
            .setSortDatanodesInPipeline(topologyAwareReadEnabled)
            .build();
        return ozoneManagerClient.lookupTablet(omTabletArgs);
      } catch (IOException e) {
        LOG.error("Unable to lookup tablet {} on retry.", tabletInfo.getTabletName(), e);
        return null;
      }
    });
  }

  @Override
  public HetuOutputStream createTablet(String databaseName, String tableName,
      String partitionName, String tabletName, long size, ReplicationConfig replicationConfig,
      boolean overWrite, boolean recursive) throws IOException {
    OmTabletArgs tabletArgs = new OmTabletArgs.Builder()
        .setDatabaseName(databaseName)
        .setTableName(tableName)
        .setPartitionName(partitionName)
        .setTabletName(tabletName)
        .setDataSize(size)
        .setType(replicationConfig.getReplicationType())
        .setFactor(ReplicationConfig.getLegacyFactor(replicationConfig))
        .setAcls(getAclList())
        .build();
    OpenTabletSession tabletSession =
        ozoneManagerClient.openTablet(tabletArgs);
    return createOutputStream(tabletSession, UUID.randomUUID().toString(),
        replicationConfig);
  }

  @Override
  public HetuOutputStream openTablet(String databaseName, String tableName, String partitionName, String tablet) {
    // TODO open tablet write mode
    return null;
  }

  @Override
  public List<OzoneTabletStatus> listStatus(String databaseName, String tableName,
      String partitionName, String tabletName, boolean recursive, String startTablet, long numEntries)
      throws IOException {
//    OmTabletArgs tabletArgs = new OmTabletArgs.Builder()
//        .setDatabaseName(databaseName)
//        .setTableName(tableName)
//        .setPartitionName(partitionName)
//         .setTabletName(tabletName)
//        .setRefreshPipeline(true)
//        .setSortDatanodesInPipeline(topologyAwareReadEnabled)
//        .build();
//    return ozoneManagerClient
//        .listStatus(tabletArgs, recursive, startTablet, numEntries);
    //TODO: list status
    return null;
  }

  /**
   * Add acl for Ozone object. Return true if acl is added successfully else
   * false.
   *
   * @param obj Ozone object for which acl should be added.
   * @param acl ozone acl to be added.
   * @throws IOException if there is error.
   */
  @Override
  public boolean addAuth(HetuObj obj, OzoneAcl acl) throws IOException {
    return ozoneManagerClient.addAuth(obj, acl);
  }

  /**
   * Remove acl for Ozone object. Return true if acl is removed successfully
   * else false.
   *
   * @param obj Ozone object.
   * @param acl Ozone acl to be removed.
   * @throws IOException if there is error.
   */
  @Override
  public boolean removeAuth(HetuObj obj, OzoneAcl acl) throws IOException {
    return ozoneManagerClient.removeAuth(obj, acl);
  }

  /**
   * Acls to be set for given Ozone object. This operations reset ACL for given
   * object to list of ACLs provided in argument.
   *
   * @param obj Ozone object.
   * @param acls List of acls.
   * @throws IOException if there is error.
   */
  @Override
  public boolean setAuth(HetuObj obj, List<OzoneAcl> acls) throws IOException {
    return ozoneManagerClient.setAuth(obj, acls);
  }

  /**
   * Returns list of ACLs for given Ozone object.
   *
   * @param obj Ozone object.
   * @throws IOException if there is error.
   */
  @Override
  public List<OzoneAcl> getAuth(HetuObj obj) throws IOException {
    return ozoneManagerClient.getAuth(obj);
  }

  private HetuInputStream createInputStream(
      OmTabletInfo tabletInfo, Function<OmTabletInfo, OmTabletInfo> retryFunction)
      throws IOException {
    // When Tablet is not MPU or when Tablet is MPU and encryption is not enabled
    // Need to revisit for GDP.
      LengthInputStream lengthInputStream = TabletInputStream
          .getFromOmTabletInfo(tabletInfo, xceiverClientManager,
              clientConfig.isChecksumVerify(), retryFunction);
      try {
        Map< String, String > tabletInfoMetadata = tabletInfo.getMetadata();
        if (Boolean.valueOf(tabletInfoMetadata.get(OzoneConsts.GDPR_FLAG))) {
          GDPRSymmetricKey gk = new GDPRSymmetricKey(
              tabletInfoMetadata.get(OzoneConsts.GDPR_SECRET),
              tabletInfoMetadata.get(OzoneConsts.GDPR_ALGORITHM)
          );
          gk.getCipher().init(Cipher.DECRYPT_MODE, gk.getSecretKey());
          return new HetuInputStream(
              new CipherInputStream(lengthInputStream, gk.getCipher()));
        }
      } catch (Exception ex) {
        throw new IOException(ex);
      }
      return new HetuInputStream(lengthInputStream.getWrappedStream());
  }

  private HetuOutputStream createOutputStream(OpenTabletSession openTablet,
      String requestId, ReplicationConfig replicationConfig)
      throws IOException {
    TabletOutputStream tabletOutputStream =
        new TabletOutputStream.Builder()
            .setHandler(openTablet)
            .setXceiverClientManager(xceiverClientManager)
            .setOmClient(ozoneManagerClient)
            .setRequestID(requestId)
            .setType(replicationConfig.getReplicationType())
            .setFactor(ReplicationConfig.getLegacyFactor(replicationConfig))
            .enableUnsafeByteBufferConversion(unsafeByteBufferConversion)
            .setConfig(clientConfig)
            .build();
    tabletOutputStream
        .addPreallocateBlocks(openTablet.getTabletInfo().getLatestVersionLocations(),
            openTablet.getOpenVersion());
      try{
        GDPRSymmetricKey gk;
        Map<String, String> openTabletMetadata =
            openTablet.getTabletInfo().getMetadata();
        if(Boolean.valueOf(openTabletMetadata.get(OzoneConsts.GDPR_FLAG))){
          gk = new GDPRSymmetricKey(
              openTabletMetadata.get(OzoneConsts.GDPR_SECRET),
              openTabletMetadata.get(OzoneConsts.GDPR_ALGORITHM)
          );
          gk.getCipher().init(Cipher.ENCRYPT_MODE, gk.getSecretKey());
          return new HetuOutputStream(
              new CipherOutputStream(tabletOutputStream, gk.getCipher()));
        }
      }catch (Exception ex){
        throw new IOException(ex);
      }

      return new HetuOutputStream(tabletOutputStream);
  }

  @Override
  public KeyProvider getKeyProvider() throws IOException {
    URI kmsUri = getKeyProviderUri();
    if (kmsUri == null) {
      return null;
    }

    try {
      return keyProviderCache.get(kmsUri, new Callable<KeyProvider>() {
        @Override
        public KeyProvider call() throws Exception {
          return OzoneKMSUtil.getKeyProvider(conf, kmsUri);
        }
      });
    } catch (Exception e) {
      LOG.error("Can't create KeyProvider for Ozone RpcClient.", e);
      return null;
    }
  }

  @Override
  public URI getKeyProviderUri() throws IOException {
    // TODO: fix me to support kms instances for difference OMs
    return OzoneKMSUtil.getKeyProviderUri(ugi,
        null, null, conf);
  }

  @Override
  public String getCanonicalServiceName() {
    return (dtService != null) ? dtService.toString() : null;
  }

  @Override
  @VisibleForTesting
  public OzoneManagerProtocol getOzoneManagerClient() {
    return ozoneManagerClient;
  }

  @VisibleForTesting
  public Cache<URI, KeyProvider> getKeyProviderCache() {
    return keyProviderCache;
  }
}
