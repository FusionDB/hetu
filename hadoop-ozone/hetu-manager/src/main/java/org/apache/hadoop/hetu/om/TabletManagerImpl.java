/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hetu.om;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.UniqueId;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.CodecRegistry;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.hetu.om.request.OMClientRequest;
import org.apache.hadoop.hetu.security.OzoneBlockTokenSecretManager;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.helpers.*;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PartKeyInfo;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.RequestContext;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.PrivilegedExceptionAction;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_KEY_PROVIDER_PATH;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_BLOCK_TOKEN_ENABLED;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_BLOCK_TOKEN_ENABLED_DEFAULT;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockTokenSecretProto.AccessModeProto.READ;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockTokenSecretProto.AccessModeProto.WRITE;
import static org.apache.hadoop.ozone.ClientVersions.CURRENT_VERSION;
import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_CONTAINER_RATIS_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_CONTAINER_RATIS_ENABLED_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_TIMEOUT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_LIST_TRASH_KEYS_MAX;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_LIST_TRASH_KEYS_MAX_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_KEY_PREALLOCATION_BLOCKS_MAX;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_KEY_PREALLOCATION_BLOCKS_MAX_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.*;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.PARTITION_LOCK;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType.KEY;
import static org.apache.hadoop.util.Time.monotonicNow;

/**
 * Implementation of tabletManager.
 */
public class TabletManagerImpl implements TabletManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(TabletManagerImpl.class);

  /**
   * A SCM block client, used to talk to SCM to allocate block during putKey.
   */
  private final OzoneManager ozoneManager;
  private final ScmClient scmClient;
  private final OMMetadataManager metadataManager;
  private final long scmBlockSize;
  private final boolean useRatis;

  private final int preallocateBlocksMax;
  private final int listTrashKeysMax;
  private final String omId;
  private final OzoneBlockTokenSecretManager secretManager;
  private final boolean grpcBlockTokenEnabled;

  private BackgroundService tabletDeletingService;

  private final KeyProviderCryptoExtension kmsProvider;
  private final PrefixManager prefixManager;

  private final boolean enableFileSystemPaths;


  @VisibleForTesting
  public TabletManagerImpl(ScmBlockLocationProtocol scmBlockClient,
                           OMMetadataManager metadataManager, OzoneConfiguration conf, String omId,
                           OzoneBlockTokenSecretManager secretManager) {
    this(null, new ScmClient(scmBlockClient, null), metadataManager,
        conf, omId, secretManager, null, null);
  }

  @VisibleForTesting
  public TabletManagerImpl(ScmBlockLocationProtocol scmBlockClient,
                           StorageContainerLocationProtocol scmContainerClient,
                           OMMetadataManager metadataManager, OzoneConfiguration conf, String omId,
                           OzoneBlockTokenSecretManager secretManager) {
    this(null, new ScmClient(scmBlockClient, scmContainerClient),
        metadataManager, conf, omId, secretManager, null, null);
  }

  public TabletManagerImpl(OzoneManager om, ScmClient scmClient,
                           OzoneConfiguration conf, String omId) {
    this (om, scmClient, om.getMetadataManager(), conf, omId,
        om.getBlockTokenMgr(), om.getKmsProvider(), om.getPrefixManager());
  }

  @SuppressWarnings("parameternumber")
  public TabletManagerImpl(OzoneManager om, ScmClient scmClient,
                           OMMetadataManager metadataManager, OzoneConfiguration conf, String omId,
                           OzoneBlockTokenSecretManager secretManager,
                           KeyProviderCryptoExtension kmsProvider, PrefixManager prefixManager) {
    this.scmBlockSize = (long) conf
        .getStorageSize(OZONE_SCM_BLOCK_SIZE, OZONE_SCM_BLOCK_SIZE_DEFAULT,
            StorageUnit.BYTES);
    this.useRatis = conf.getBoolean(DFS_CONTAINER_RATIS_ENABLED_KEY,
        DFS_CONTAINER_RATIS_ENABLED_DEFAULT);
    this.preallocateBlocksMax = conf.getInt(
        OZONE_KEY_PREALLOCATION_BLOCKS_MAX,
        OZONE_KEY_PREALLOCATION_BLOCKS_MAX_DEFAULT);
    this.grpcBlockTokenEnabled = conf.getBoolean(
        HDDS_BLOCK_TOKEN_ENABLED,
        HDDS_BLOCK_TOKEN_ENABLED_DEFAULT);
    this.listTrashKeysMax = conf.getInt(
      OZONE_CLIENT_LIST_TRASH_KEYS_MAX,
      OZONE_CLIENT_LIST_TRASH_KEYS_MAX_DEFAULT);
    this.enableFileSystemPaths =
        conf.getBoolean(OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS,
            OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS_DEFAULT);

    this.ozoneManager = om;
    this.omId = omId;
    this.scmClient = scmClient;
    this.metadataManager = metadataManager;
    this.prefixManager = prefixManager;
    this.secretManager = secretManager;
    this.kmsProvider = kmsProvider;

  }

  @Override
  public void start(OzoneConfiguration configuration) {
    if (tabletDeletingService == null) {
      long blockDeleteInterval = configuration.getTimeDuration(
          OZONE_BLOCK_DELETING_SERVICE_INTERVAL,
          OZONE_BLOCK_DELETING_SERVICE_INTERVAL_DEFAULT,
          TimeUnit.MILLISECONDS);
      long serviceTimeout = configuration.getTimeDuration(
          OZONE_BLOCK_DELETING_SERVICE_TIMEOUT,
          OZONE_BLOCK_DELETING_SERVICE_TIMEOUT_DEFAULT,
          TimeUnit.MILLISECONDS);
      tabletDeletingService = new TabletDeletingService(ozoneManager,
          scmClient.getBlockClient(), this, blockDeleteInterval,
          serviceTimeout, configuration);
      tabletDeletingService.start();
    }
  }

  KeyProviderCryptoExtension getKMSProvider() {
    return kmsProvider;
  }

  @Override
  public void stop() throws IOException {
    if (tabletDeletingService != null) {
      tabletDeletingService.shutdown();
      tabletDeletingService = null;
    }
  }

  private OmPartitionInfo getPartitionInfo(String databaseName, String tableName,
                                     String partitionName)
      throws IOException {
    String partitionKey = metadataManager.getPartitionKey(databaseName, tableName,
            partitionName);
    return metadataManager.getPartitionTable().get(partitionKey);
  }

  private void validatePartition(String databaseName, String tableName,
                                 String partitionName)
      throws IOException {
    String partitionKey = metadataManager.getPartitionKey(databaseName, tableName,
            partitionName);
    // Check if partition exists
    if (metadataManager.getPartitionTable().get(partitionKey) == null) {
      String tableKey = metadataManager.getMetaTableKey(databaseName, tableName);
      // If the table also does not exist, we should throw table not found
      // exception
      if (metadataManager.getMetaTable().get(tableKey) == null) {
        LOG.error("table not found: {}", tableName);
        throw new OMException("Table not found",
            TABLE_NOT_FOUND);
      }

      String databaseKey = metadataManager.getDatabaseKey(databaseName);
      if (metadataManager.getPartitionTable().get(databaseKey) == null) {
        LOG.error("database not found: {}", databaseName);
        throw new OMException("Database not found", DATABASE_NOT_FOUND);
      }

      // if the database and table exists but partition does not exist, throw partition not found
      // exception
      LOG.error("partition not found: {}/{}/{} ", databaseName, tableName, partitionName);
      throw new OMException("Partition not found",
          PARTITION_NOT_FOUND);
    }
  }

  @Override
  public OmTabletLocationInfo allocateTablet(OmTabletArgs args, long clientID,
      ExcludeList excludeList) throws IOException {
    Preconditions.checkNotNull(args);


    String databaseName = args.getDatabaseName();
    String tableName = args.getTableName();
    String partitionName = args.getPartitionName();
    String tabletName = args.getTabletName();
    validatePartition(databaseName, tableName, partitionName);
    String openTablet = metadataManager.getOpenTablet(
        databaseName, tableName, partitionName, tabletName, clientID);

    OmTabletInfo tabletInfo = metadataManager.getOpenTabletTable().get(openTablet);
    if (tabletInfo == null) {
      LOG.error("Allocate block for a tablet not in open status in meta store" +
          " /{}/{}/{}/{} with ID {}", databaseName, tableName, partitionName, tabletName, clientID);
      throw new OMException("Open Tablet not found",
          TABLET_NOT_FOUND);
    }

    // current version not committed, so new blocks coming now are added to
    // the same version
    List<OmTabletLocationInfo> locationInfos =
        allocateBlock(tabletInfo, excludeList, scmBlockSize);

    tabletInfo.appendNewBlocks(locationInfos, true);
    tabletInfo.updateModifcationTime();
    metadataManager.getOpenTabletTable().put(openTablet, tabletInfo);

    return locationInfos.get(0);
  }

  /**
   * This methods avoids multiple rpc calls to SCM by allocating multiple blocks
   * in one rpc call.
   * @param tabletInfo - tablet info for tablet to be allocated.
   * @param requestedSize requested length for allocation.
   * @param excludeList exclude list while allocating blocks.
   * @param requestedSize requested size to be allocated.
   * @return
   * @throws IOException
   */
  private List<OmTabletLocationInfo> allocateBlock(OmTabletInfo tabletInfo,
      ExcludeList excludeList, long requestedSize) throws IOException {
    int numBlocks = Math.min((int) ((requestedSize - 1) / scmBlockSize + 1),
        preallocateBlocksMax);
    List<OmTabletLocationInfo> locationInfos = new ArrayList<>(numBlocks);
    String remoteUser = getRemoteUser().getShortUserName();
    List<AllocatedBlock> allocatedBlocks;
    try {
      allocatedBlocks = scmClient.getBlockClient()
          .allocateBlock(scmBlockSize, numBlocks, tabletInfo.getType(),
              tabletInfo.getFactor(), omId, excludeList);
    } catch (SCMException ex) {
      if (ex.getResult()
          .equals(SCMException.ResultCodes.SAFE_MODE_EXCEPTION)) {
        throw new OMException(ex.getMessage(), ResultCodes.SCM_IN_SAFE_MODE);
      }
      throw ex;
    }
    for (AllocatedBlock allocatedBlock : allocatedBlocks) {
      BlockID blockID = new BlockID(allocatedBlock.getBlockID());
      OmTabletLocationInfo.Builder builder = new OmTabletLocationInfo.Builder()
          .setBlockID(blockID)
          .setLength(scmBlockSize)
          .setOffset(0)
          .setPipeline(allocatedBlock.getPipeline());
      if (grpcBlockTokenEnabled) {
        builder.setToken(secretManager
            .generateToken(remoteUser, blockID,
                EnumSet.of(READ, WRITE), scmBlockSize));
      }
      locationInfos.add(builder.build());
    }
    return locationInfos;
  }

  /* Optimize ugi lookup for RPC operations to avoid a trip through
   * UGI.getCurrentUser which is synch'ed.
   */
  public static UserGroupInformation getRemoteUser() throws IOException {
    UserGroupInformation ugi = Server.getRemoteUser();
    return (ugi != null) ? ugi : UserGroupInformation.getCurrentUser();
  }

  private EncryptedKeyVersion generateEDEK(
      final String ezKeyName) throws IOException {
    if (ezKeyName == null) {
      return null;
    }
    long generateEDEKStartTime = monotonicNow();
    EncryptedKeyVersion edek = SecurityUtil.doAsLoginUser(
        new PrivilegedExceptionAction<EncryptedKeyVersion>() {
          @Override
          public EncryptedKeyVersion run() throws IOException {
            try {
              return getKMSProvider().generateEncryptedKey(ezKeyName);
            } catch (GeneralSecurityException e) {
              throw new IOException(e);
            }
          }
        });
    long generateEDEKTime = monotonicNow() - generateEDEKStartTime;
    LOG.debug("generateEDEK takes {} ms", generateEDEKTime);
    Preconditions.checkNotNull(edek);
    return edek;
  }

  @Override
  public OpenTabletSession openTablet(OmTabletArgs args) throws IOException {
    Preconditions.checkNotNull(args);
    Preconditions.checkNotNull(args.getAcls(), "Default acls " +
        "should be set.");

    String databaseName = args.getDatabaseName();
    String tableName = args.getTableName();
    String partitionName = args.getPartitionName();
    String tabletName = args.getTabletName();
    validatePartition(databaseName, tableName, partitionName);

    long currentTime = UniqueId.next();
    OmTabletInfo tabletInfo;
    long openVersion;
    // NOTE size of a tablet is not a hard limit on anything, it is a value that
    // client should expect, in terms of current size of tablet. If client sets
    // a value, then this value is used, otherwise, we allocate a single
    // block which is the current size, if read by the client.
    final long size = args.getDataSize() > 0 ?
        args.getDataSize() : scmBlockSize;
    final List<OmTabletLocationInfo> locations = new ArrayList<>();

    ReplicationFactor factor = args.getFactor();
    if (factor == null) {
      factor = useRatis ? ReplicationFactor.THREE : ReplicationFactor.ONE;
    }

    ReplicationType type = args.getType();
    if (type == null) {
      type = useRatis ? ReplicationType.RATIS : ReplicationType.STAND_ALONE;
    }

    String dbTabletName = metadataManager.getOzoneTablet(
        args.getDatabaseName(), args.getTableName(), args.getPartitionName(),
        args.getTabletName());

    metadataManager.getLock().acquireWriteLock(PARTITION_LOCK, databaseName,
        tableName, partitionName);
    OmPartitionInfo partitionInfo;
    try {
      partitionInfo = getPartitionInfo(databaseName, tableName, partitionName);
      // the tablet if exist return are this openTabletSession
      OmTabletInfo dbTabletInfo = metadataManager.getTabletTable().get(dbTabletName);
      if (metadataManager.getTabletTable().isExist(dbTabletName) &&
              dbTabletInfo.getTabletLocationVersions().size() > 0) {
        long currentVersion = dbTabletInfo.getLatestVersionLocations().getVersion();
        return new OpenTabletSession(currentTime, dbTabletInfo, currentVersion);
      }
      tabletInfo = prepareTabletInfo(args, dbTabletName, size, locations);
    } catch (OMException e) {
      throw e;
    } catch (IOException ex) {
      LOG.error("Tablet open failed for database:{} table:{} partition: {} tablet:{}",
          databaseName, tableName, partitionName, tabletName, ex);
      throw new OMException(ex.getMessage(), ResultCodes.TABLET_ALLOCATION_ERROR);
    } finally {
      metadataManager.getLock().releaseWriteLock(PARTITION_LOCK, databaseName,
          tableName, partitionName);
    }
    if (tabletInfo == null) {
      // the tablet does not exist, create a new object, the new blocks are the
      // version 0
      tabletInfo = createTabletInfo(args, locations, factor, type, size,
          partitionInfo);
    }
    openVersion = tabletInfo.getLatestVersionLocations().getVersion();
    LOG.debug("Tablet {} allocated in database {} table {} partition {}",
        tabletName, databaseName, tableName, partitionName);
    allocateBlockInKey(tabletInfo, size, currentTime);
    return new OpenTabletSession(currentTime, tabletInfo, openVersion);
  }

  private void allocateBlockInKey(OmTabletInfo tabletInfo, long size, long sessionId)
      throws IOException {
    String openTablet = metadataManager
        .getOpenTablet(tabletInfo.getDatabaseName(), tabletInfo.getTableName(),
            tabletInfo.getPartitionName(), tabletInfo.getTabletName(), sessionId);
    // requested size is not required but more like a optimization:
    // SCM looks at the requested, if it 0, no block will be allocated at
    // the point, if client needs more blocks, client can always call
    // allocateBlock. But if requested size is not 0, OM will preallocate
    // some blocks and piggyback to client, to save RPC calls.
    // TODO: Only allocate once block in LStore or CStore
    if (size > 0 && tabletInfo.getTabletLocationVersions().size() == 0) {
      List<OmTabletLocationInfo> locationInfos =
          allocateBlock(tabletInfo, new ExcludeList(), size);
      tabletInfo.appendNewBlocks(locationInfos, true);
    }

    metadataManager.getOpenTabletTable().put(openTablet, tabletInfo);

  }

  private OmTabletInfo prepareTabletInfo(
      OmTabletArgs tabletArgs, String dbTabletName, long size,
      List<OmTabletLocationInfo> locations)
      throws IOException {
    OmTabletInfo tabletInfo = metadataManager.getTabletTable().get(dbTabletName);
    // the tablet already exist, the new blocks will be added as new version
    // when locations.size = 0, the new version will have identical blocks
    // as its previous version
    tabletInfo.addNewVersion(locations, true);
    tabletInfo.setDataSize(size + tabletInfo.getDataSize());
    if(tabletInfo != null) {
      tabletInfo.setMetadata(tabletArgs.getMetadata());
    }
    return tabletInfo;
  }

  /**
   * Create OmTabletInfo object.
   * @param tabletArgs
   * @param locations
   * @param factor
   * @param type
   * @param size
   * @param omPartitionInfo
   * @return
   */
  private OmTabletInfo createTabletInfo(OmTabletArgs tabletArgs,
      List<OmTabletLocationInfo> locations,
      ReplicationFactor factor,
      ReplicationType type, long size,
      OmPartitionInfo omPartitionInfo) {
    OmTabletInfo.Builder builder = new OmTabletInfo.Builder()
        .setDatabaseName(tabletArgs.getDatabaseName())
        .setTableName(tabletArgs.getTableName())
        .setPartitionName(tabletArgs.getPartitionName())
        .setTabletName(tabletArgs.getTabletName())
        .setOmTabletLocationInfos(Collections.singletonList(
            new OmTabletLocationInfoGroup(0, locations)))
        .setCreationTime(Time.now())
        .setModificationTime(Time.now())
        .setDataSize(size)
        .setReplicationType(type)
        .setReplicationFactor(factor)
        .addAllMetadata(tabletArgs.getMetadata());

    if(Boolean.valueOf(omPartitionInfo.getMetadata().get(OzoneConsts.GDPR_FLAG))) {
      builder.addMetadata(OzoneConsts.GDPR_FLAG, Boolean.TRUE.toString());
    }
    return builder.build();
  }

  @Override
  public void commitTablet(OmTabletArgs args, long clientID) throws IOException {
    Preconditions.checkNotNull(args);
    String databaseName = args.getDatabaseName();
    String tableName = args.getTableName();
    String partitionName = args.getPartitionName();
    String tabletName = args.getTabletName();
    List<OmTabletLocationInfo> locationInfoList = args.getLocationInfoList();
    String objectTablet = metadataManager
        .getOzoneTablet(databaseName, tableName, partitionName, tabletName);
    String openTablet = metadataManager
        .getOpenTablet(databaseName, tableName, partitionName, tabletName, clientID);
    Preconditions.checkNotNull(locationInfoList);
    try {
      metadataManager.getLock().acquireWriteLock(PARTITION_LOCK, databaseName, tableName,
          partitionName);
      validatePartition(databaseName, tableName, partitionName);
      OmTabletInfo tabletInfo = metadataManager.getOpenTabletTable().get(openTablet);
      if (tabletInfo == null) {
        throw new OMException("Failed to commit tablet, as " + openTablet + "entry " +
            "is not found in the openTablet table", TABLET_NOT_FOUND);
      }
      tabletInfo.setDataSize(args.getDataSize());
      tabletInfo.setModificationTime(Time.now());

      //update the block length for each block
      tabletInfo.updateLocationInfoList(locationInfoList, false);
      metadataManager.getStore().move(
          openTablet,
          objectTablet,
          tabletInfo,
          metadataManager.getOpenTabletTable(),
          metadataManager.getTabletTable());
    } catch (OMException e) {
      throw e;
    } catch (IOException ex) {
      LOG.error("Tablet commit failed for database:{} table:{} partition:{} tablet: {}",
          databaseName, tableName, partitionName, tabletName, ex);
      throw new OMException(ex.getMessage(),
          ResultCodes.TABLET_ALLOCATION_ERROR);
    } finally {
      metadataManager.getLock().releaseWriteLock(PARTITION_LOCK, databaseName,
          tableName, partitionName);
    }
  }

  @Override
  public OmTabletInfo lookupTablet(OmTabletArgs args, String clientAddress)
      throws IOException {
    Preconditions.checkNotNull(args);
    String databaseName = args.getDatabaseName();
    String tableName = args.getTableName();
    String partitionName = args.getPartitionName();
    String tabletName = OMClientRequest.validateAndNormalizeTablet(
        enableFileSystemPaths, args.getTabletName());
    metadataManager.getLock().acquireReadLock(PARTITION_LOCK, databaseName,
        tableName, partitionName);
    OmTabletInfo value = null;
    try {
      String tabletBytes = metadataManager.getOzoneTablet(
          databaseName, tableName, partitionName, tabletName);
      value = metadataManager.getTabletTable().get(tabletBytes);
    } catch (IOException ex) {
      if (ex instanceof OMException) {
        throw ex;
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Get tablet failed for database:{} table:{} partition:{} tablet: {}", databaseName,
                tableName, partitionName, tabletName, ex);
      }
      throw new OMException(ex.getMessage(), TABLET_NOT_FOUND);
    } finally {
      metadataManager.getLock().releaseReadLock(PARTITION_LOCK, databaseName,
          tableName, partitionName);
    }

    if (value == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("database:{} table:{} partition:{} tablet:{} not found", databaseName,
                tableName, partitionName, tabletName);
      }
      throw new OMException("Tablet not found", TABLET_NOT_FOUND);
    }

    // add block token for read.
    addBlockToken4Read(value);

    // Refresh container pipeline info from SCM
    // based on OmTabletArgs.refreshPipeline flag
    // value won't be null as the check is done inside try/catch block.
    refresh(value);

    if (args.getSortDatanodes()) {
      sortDatanodes(clientAddress, value);
    }
    return value;
  }

  private void addBlockToken4Read(OmTabletInfo value) throws IOException {
    Preconditions.checkNotNull(value, "OMTabletInfo cannot be null");
    if (grpcBlockTokenEnabled) {
      String remoteUser = getRemoteUser().getShortUserName();
      for (OmTabletLocationInfoGroup key : value.getTabletLocationVersions()) {
        key.getLocationList().forEach(k -> {
          k.setToken(secretManager.generateToken(remoteUser, k.getBlockID(),
              EnumSet.of(READ), k.getLength()));
        });
      }
    }
  }
  /**
   * Refresh pipeline info in OM by asking SCM.
   * @param tabletList a list of OmTabletInfo
   */
  @VisibleForTesting
  protected void refreshPipeline(List<OmTabletInfo> tabletList) throws IOException {
    if (tabletList == null || tabletList.isEmpty()) {
      return;
    }

    Set<Long> containerIDs = new HashSet<>();
    for (OmTabletInfo tabletInfo : tabletList) {
      List<OmTabletLocationInfoGroup> locationInfoGroups =
          tabletInfo.getTabletLocationVersions();

      for (OmTabletLocationInfoGroup tablet : locationInfoGroups) {
        for (OmTabletLocationInfo k : tablet.getLocationList()) {
          containerIDs.add(k.getContainerID());
        }
      }
    }

    Map<Long, ContainerWithPipeline> containerWithPipelineMap =
        refreshPipeline(containerIDs);

    for (OmTabletInfo tabletInfo : tabletList) {
      List<OmTabletLocationInfoGroup> locationInfoGroups =
          tabletInfo.getTabletLocationVersions();
      for (OmTabletLocationInfoGroup tablet : locationInfoGroups) {
        for (OmTabletLocationInfo k : tablet.getLocationList()) {
          ContainerWithPipeline cp =
              containerWithPipelineMap.get(k.getContainerID());
          if (cp != null && !cp.getPipeline().equals(k.getPipeline())) {
            k.setPipeline(cp.getPipeline());
          }
        }
      }
    }
  }

  @VisibleForTesting
  protected void refreshPipelineWithTablet(List<OmTabletInfo> tabletList) throws IOException {
    if (tabletList == null || tabletList.isEmpty()) {
      return;
    }

    Set<Long> containerIDs = new HashSet<>();
    for (OmTabletInfo tabletInfo : tabletList) {
      List<OmTabletLocationInfoGroup> locationInfoGroups =
              tabletInfo.getTabletLocationVersions();

      for (OmTabletLocationInfoGroup key : locationInfoGroups) {
        for (OmTabletLocationInfo k : key.getLocationList()) {
          containerIDs.add(k.getContainerID());
        }
      }
    }

    Map<Long, ContainerWithPipeline> containerWithPipelineMap =
            refreshPipeline(containerIDs);

    for (OmTabletInfo tabletInfo : tabletList) {
      List<OmTabletLocationInfoGroup> locationInfoGroups =
              tabletInfo.getTabletLocationVersions();
      for (OmTabletLocationInfoGroup tablet : locationInfoGroups) {
        for (OmTabletLocationInfo t : tablet.getLocationList()) {
          ContainerWithPipeline cp =
                  containerWithPipelineMap.get(t.getContainerID());
          if (cp != null && !cp.getPipeline().equals(t.getPipeline())) {
            t.setPipeline(cp.getPipeline());
          }
        }
      }
    }
  }

  /**
   * Refresh pipeline info in OM by asking SCM.
   * @param containerIDs a set of containerIDs
   */
  @VisibleForTesting
  protected Map<Long, ContainerWithPipeline> refreshPipeline(
      Set<Long> containerIDs) throws IOException {
    // TODO: fix Some tests that may not initialize container client
    // The production should always have containerClient initialized.
    if (scmClient.getContainerClient() == null ||
        containerIDs == null || containerIDs.isEmpty()) {
      return Collections.EMPTY_MAP;
    }

    Map<Long, ContainerWithPipeline> containerWithPipelineMap = new HashMap<>();

    try {
      List<ContainerWithPipeline> cpList = scmClient.getContainerClient().
          getContainerWithPipelineBatch(new ArrayList<>(containerIDs));
      for (ContainerWithPipeline cp : cpList) {
        containerWithPipelineMap.put(
            cp.getContainerInfo().getContainerID(), cp);
      }
      return containerWithPipelineMap;
    } catch (IOException ioEx) {
      LOG.debug("Get containerPipeline failed for {}",
          containerIDs.toString(), ioEx);
      throw new OMException(ioEx.getMessage(), SCM_GET_PIPELINE_EXCEPTION);
    }
  }

  @Override
  public void renameTablet(OmTabletArgs args, String toTabletName) throws IOException {
    Preconditions.checkNotNull(args);
    Preconditions.checkNotNull(toTabletName);
    String databaseName = args.getDatabaseName();
    String tableName = args.getTableName();
    String partitionName = args.getPartitionName();
    String fromTabletName = args.getTabletName();
    if (toTabletName.length() == 0 || fromTabletName.length() == 0) {
      LOG.error("Rename tablet failed for database:{} table:{} partition:{} fromTablet:{} toTablet:{}",
          databaseName, tableName, partitionName, fromTabletName, toTabletName);
      throw new OMException("Tablet name is empty",
          ResultCodes.INVALID_TABLET_NAME);
    }

    metadataManager.getLock().acquireWriteLock(PARTITION_LOCK, databaseName,
        tableName, partitionName);
    try {
      // fromTabletName should exist
      String fromTablet = metadataManager.getOzoneTablet(
          databaseName, tableName, partitionName, fromTabletName);
      OmTabletInfo fromTabletValue = metadataManager.getTabletTable().get(fromTablet);
      if (fromTabletValue == null) {
        // TODO: Add support for renaming open tablet
        LOG.error(
            "Rename tablet failed for database:{} table:{} partition: {} formTablet:{} toTablet:{}. "
                + "Tablet: {} not found.", databaseName, tableName, partitionName, fromTabletName,
            toTabletName, fromTabletName);
        throw new OMException("Tablet not found",
            TABLET_NOT_FOUND);
      }

      // A rename is a no-op if the target and source name is same.
      // TODO: Discuss if we need to throw?.
      if (fromTabletName.equals(toTabletName)) {
        return;
      }

      // toTabletName should not exist
      String toTablet =
          metadataManager.getOzoneTablet(databaseName, tableName, partitionName, toTabletName);
      OmTabletInfo toTabletValue = metadataManager.getTabletTable().get(toTablet);
      if (toTabletValue != null) {
        LOG.error(
            "Rename tablet failed for database:{} table:{} partition:{} fromTablet:{} toTablet:{}. "
                + "Tablet: {} already exists.", databaseName, tableName, partitionName,
            fromTabletName, toTabletName, toTabletName);
        throw new OMException("Tablet already exists",
            ResultCodes.TABLET_ALREADY_EXISTS);
      }

      fromTabletValue.setTabletName(toTabletName);
      fromTabletValue.updateModifcationTime();
      DBStore store = metadataManager.getStore();
      try (BatchOperation batch = store.initBatchOperation()) {
        metadataManager.getTabletTable().deleteWithBatch(batch, fromTablet);
        metadataManager.getTabletTable().putWithBatch(batch, toTablet,
            fromTabletValue);
        store.commitBatchOperation(batch);
      }
    } catch (IOException ex) {
      if (ex instanceof OMException) {
        throw ex;
      }
      LOG.error("Rename tablet failed for database:{} table:{} partition:{} fromTablet:{} toTablet:{}",
          databaseName, tableName, partitionName, fromTabletName, toTabletName, ex);
      throw new OMException(ex.getMessage(),
          ResultCodes.TABLET_RENAME_ERROR);
    } finally {
      metadataManager.getLock().releaseWriteLock(PARTITION_LOCK, databaseName,
          tableName, partitionName);
    }
  }

  @Override
  public void deleteTablet(OmTabletArgs args) throws IOException {
    Preconditions.checkNotNull(args);
    String databaseName = args.getDatabaseName();
    String tableName = args.getTableName();
    String partitionName = args.getPartitionName();
    String tabletName = args.getTabletName();
    metadataManager.getLock().acquireWriteLock(PARTITION_LOCK, databaseName,
        tableName, partitionName);
    try {
      String objectTablet = metadataManager.getOzoneTablet(
          databaseName, tableName, partitionName, tabletName);
      OmTabletInfo tabletInfo = metadataManager.getTabletTable().get(objectTablet);
      if (tabletInfo == null) {
        throw new OMException("Tablet not found",
            TABLET_NOT_FOUND);
      } else {
        // directly delete tablet with no blocks from db. This tablet need not be
        // moved to deleted table.
        if (isTabletEmpty(tabletInfo)) {
          metadataManager.getTabletTable().delete(objectTablet);
          LOG.debug("Tablet {} deleted from OM DB", tabletName);
          return;
        }
      }
      RepeatedOmTabletInfo repeatedOmTabletInfo =
          metadataManager.getDeletedTablet().get(objectTablet);
      repeatedOmTabletInfo = OmUtils.prepareTabletForDelete(tabletInfo,
          repeatedOmTabletInfo, 0L, false);
      metadataManager.getTabletTable().delete(objectTablet);
      metadataManager.getDeletedTablet().put(objectTablet, repeatedOmTabletInfo);
    } catch (OMException ex) {
      throw ex;
    } catch (IOException ex) {
      LOG.error(String.format("Delete tablet failed for database:%s "
          + "table:%s partition:%s tablet:%s", databaseName, tableName, partitionName, tabletName), ex);
      throw new OMException(ex.getMessage(), ex,
          ResultCodes.TABLET_DELETION_ERROR);
    } finally {
      metadataManager.getLock().releaseWriteLock(PARTITION_LOCK, databaseName,
          tableName, partitionName);
    }
  }

  private boolean isTabletEmpty(OmTabletInfo tabletInfo) {
    for (OmTabletLocationInfoGroup tabletLocationList : tabletInfo
        .getTabletLocationVersions()) {
      if (tabletLocationList.getLocationListCount() != 0) {
        return false;
      }
    }
    return true;
  }

  @Override
  public List<OmTabletInfo> listTablets(String databaseName, String tableName,
      String partitionName, String startTablet, String tabletPrefix,
      int maxNumTablets) throws IOException {
    Preconditions.checkNotNull(databaseName);
    Preconditions.checkNotNull(tableName);
    Preconditions.checkNotNull(partitionName);

    // We don't take a lock in this path, since we walk the
    // underlying table using an iterator. That automatically creates a
    // snapshot of the data, so we don't need these locks at a higher level
    // when we iterate.

    if (enableFileSystemPaths) {
      startTablet = OmUtils.normalizeTablet(startTablet, true);
      tabletPrefix = OmUtils.normalizeTablet(tabletPrefix, true);
    }

    List<OmTabletInfo> tabletList = metadataManager.listTablets(databaseName, tableName,
        partitionName, startTablet, tabletPrefix, maxNumTablets);

    return tabletList;
  }

  @Override
  public List<RepeatedOmTabletInfo> listTrash(String databaseName,
      String tableName, String partitionName, String startTabletName, String tabletPrefix,
      int maxNumTablets) throws IOException {

    Preconditions.checkNotNull(databaseName);
    Preconditions.checkNotNull(tableName);
    Preconditions.checkNotNull(partitionName);
    Preconditions.checkArgument(maxNumTablets <= listTrashKeysMax,
        "The max tablets limit specified is not less than the cluster " +
          "allowed maximum limit.");
  // TODO
  return null;
  }

  @Override
  public List<BlockGroup> getPendingDeletionTablets(final int count)
      throws IOException {
    return  metadataManager.getPendingDeletionTablets(count);
  }

  @Override
  public List<String> getExpiredOpenTablets(int count) throws IOException {
    return metadataManager.getExpiredOpenTablets(count);
  }

  @Override
  public void deleteExpiredOpenTablet(String objectTabletName) throws IOException {
    Preconditions.checkNotNull(objectTabletName);
    // TODO: Fix this in later patches.
  }

  @Override
  public OMMetadataManager getMetadataManager() {
    return metadataManager;
  }

  @Override
  public BackgroundService getDeletingService() {
    return tabletDeletingService;
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
  public boolean addAcl(OzoneObj obj, OzoneAcl acl) throws IOException {
    // TODO:
    return false;
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
  public boolean removeAcl(OzoneObj obj, OzoneAcl acl) throws IOException {
    // TODO:
    return false;
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
  public boolean setAcl(OzoneObj obj, List<OzoneAcl> acls) throws IOException {
    // TODO:
    return false;
  }

  /**
   * Returns list of ACLs for given Ozone object.
   *
   * @param obj Ozone object.
   * @throws IOException if there is error.
   */
  @Override
  public List<OzoneAcl> getAcl(OzoneObj obj) throws IOException {
    // TODO:
    return new ArrayList<>();
  }

  /**
   * Check access for given ozoneObject.
   *
   * @param ozObject object for which access needs to be checked.
   * @param context Context object encapsulating all user related information.
   * @return true if user has access else false.
   */
  @Override
  public boolean checkAccess(OzoneObj ozObject, RequestContext context)
      throws OMException {
    // TODO:
    return false;
  }

  /**
   * Ozone tablet api to get block status for an entry.
   *
   * @param args Tablet args
   * @throws OMException if tablet does not exist
   *                     if partition does not exist
   *                     if table does not exist
   *                     if database does not exist
   * @throws IOException if there is error in the db
   *                     invalid arguments
   */
  @Override
  public OzoneTabletStatus getTabletStatus(OmTabletArgs args) throws IOException {
    Preconditions.checkNotNull(args, "Tablet args can not be null");
    return getTabletStatus(args, null);
  }

  /**
   * Ozone Tablet api to get block status for an entry.
   *
   * @param args Tablet args
   * @param clientAddress a hint to tablet manager, order the datanode in returned
   *                      pipeline by distance between client and datanode.
   * @throws OMException if tablet does not exist
   *                     if partition does not exist
   *                     if table does not exist
   *                     if database does not exist
   * @throws IOException if there is error in the db
   *                     invalid arguments
   */
  @Override
  public OzoneTabletStatus getTabletStatus(OmTabletArgs args, String clientAddress)
          throws IOException {
    Preconditions.checkNotNull(args, "Tablet args can not be null");
    String databaseName = args.getDatabaseName();
    String tableName = args.getTableName();
    String partitionName = args.getPartitionName();
    String tabletName = args.getTabletName();

    return getOzoneTabletStatus(databaseName, tableName, partitionName, tabletName,
            args.getRefreshPipeline(), args.getSortDatanodes(), clientAddress);
  }

  private OzoneTabletStatus getOzoneTabletStatus(String databaseName,
                                             String tableName,
                                             String partitionName,
                                             String tabletName,
                                             boolean refreshPipeline,
                                             boolean sortDatanodes,
                                             String clientAddress)
      throws IOException {
    OmTabletInfo tabletInfo = null;
    metadataManager.getLock().acquireReadLock(PARTITION_LOCK, databaseName,
        tableName, partitionName);
    try {
      // Check if the tablet is a block.
      String tabletBytes = metadataManager.getOzoneTablet(
              databaseName, tableName, partitionName, tabletName);
      tabletInfo = metadataManager.getTabletTable().get(tabletBytes);
      if (tabletInfo == null) {
        // Tablet is not found, throws exception
        if (LOG.isDebugEnabled()) {
          LOG.debug("Unable to get block status for the tablet: database: {}, table:" +
                          " {}, partition: {} tablet: {}, with error: No such tablet exists.",
                  databaseName, tableName, partitionName, tabletName);
        }
        throw new OMException("Unable to get tablet status: database: " +
                databaseName + " table: " + tableName + " partition: " +
                partitionName + " tablet: "+ tabletName,
                TABLET_NOT_FOUND);
      } else {
        // refreshPipeline flag check has been removed as part of
        // https://issues.apache.org/jira/browse/HDDS-3658.
        // Please refer this jira for more details.
        refresh(tabletInfo);
        if (sortDatanodes) {
          sortDatanodes(clientAddress, tabletInfo);
        }
        return new OzoneTabletStatus(tabletInfo, scmBlockSize);
      }
    } finally {
      metadataManager.getLock().releaseReadLock(PARTITION_LOCK, databaseName,
              tableName, partitionName);
    }
  }

  /**
   * Refresh the tablet location information by get latest info from SCM.
   * @param tablet
   */
  @Override
  public void refresh(OmTabletInfo tablet) throws IOException {
    Preconditions.checkNotNull(tablet, "Tablet info can not be null");
    refreshPipelineWithTablet(Arrays.asList(tablet));
  }

  /**
   * Helper function for listStatus to find tablet in TableCache.
   */
  private void listStatusFindTabletInTableCache(
      Iterator<Map.Entry<CacheKey<String>, CacheValue<OmTabletInfo>>> cacheIter,
      String tabletKey, String startCacheTabletKey,
      TreeMap<String, OzoneTabletStatus> cacheTabletMap, Set<String> deletedTabletSet) {

    while (cacheIter.hasNext()) {
      Map.Entry<CacheKey<String>, CacheValue<OmTabletInfo>> entry =
          cacheIter.next();
      String cacheTabletKey = entry.getKey().getCacheKey();
      if (cacheTabletKey.equals(tabletKey)) {
        continue;
      }
      OmTabletInfo cacheOmTabletInfo = entry.getValue().getCacheValue();
      // cacheOmTabletInfo is null if an entry is deleted in cache
      if (cacheOmTabletInfo != null) {
        if (cacheTabletKey.startsWith(startCacheTabletKey) &&
            cacheTabletKey.compareTo(startCacheTabletKey) >= 0) {
          OzoneTabletStatus tabletStatus = new OzoneTabletStatus(
              cacheOmTabletInfo, scmBlockSize);
          cacheTabletMap.put(cacheTabletKey, tabletStatus);
        }
      } else {
        deletedTabletSet.add(cacheTabletKey);
      }
    }
  }

  /**
   * List the status for a tablet and its contents.
   *
   * @param args       Tablet args
   * @param startTablet   Tablet from which listing needs to start. If startTablet exists
   *                   its status is included in the final list.
   * @param numEntries Number of entries to list from the start tablet
   * @return list of tablet status
   */
  @Override
  public List<OzoneTabletStatus> listStatus(OmTabletArgs args,
                                          String startTablet, long numEntries)
          throws IOException {
    return listStatus(args, startTablet, numEntries, null);
  }

  /**
   * List the status for a tablet and its contents.
   *
   * @param args       Tablet args
   * @param startTablet   Tablet from which listing needs to start. If startTablet exists
   *                   its status is included in the final list.
   * @param numEntries Number of entries to list from the start key
   * @param clientAddress a hint to key manager, order the datanode in returned
   *                      pipeline by distance between client and datanode.
   * @return list of file status
   */
  @Override
  public List<OzoneTabletStatus> listStatus(OmTabletArgs args,
      String startTablet, long numEntries, String clientAddress)
          throws IOException {
    Preconditions.checkNotNull(args, "Tablet args can not be null");

    List<OzoneTabletStatus> tabletStatusList = new ArrayList<>();
    if (numEntries <= 0) {
      return tabletStatusList;
    }

    String databaseName = args.getDatabaseName();
    String tableName = args.getTableName();
    String partitionName = args.getPartitionName();
    String tabletName = args.getTabletName();
    // A map sorted by OmTablet to combine results from TableCache and DB.
    TreeMap<String, OzoneTabletStatus> cacheTabletKeyMap = new TreeMap<>();
    // A set to keep track of tablets deleted in cache but not flushed to DB.
    Set<String> deletedTabletSet = new TreeSet<>();

    if (Strings.isNullOrEmpty(startTablet)) {
      // tabletName is a directory
      startTablet = OzoneFSUtils.addTrailingSlashIfNeeded(tabletName);
    }

    metadataManager.getLock().acquireReadLock(PARTITION_LOCK, databaseName,
        tableName, partitionName);
    try {
      Table tabletTable = metadataManager.getTabletTable();
      Iterator<Map.Entry<CacheKey<String>, CacheValue<OmTabletInfo>>>
          cacheIter = tabletTable.cacheIterator();
      String startCacheTablet = OZONE_URI_DELIMITER + databaseName +
          OZONE_URI_DELIMITER + tableName +
          OZONE_URI_DELIMITER + partitionName + OZONE_URI_DELIMITER +
          ((startTablet.equals(OZONE_URI_DELIMITER)) ? "" : startTablet);
      // Note: eliminating the case where startCacheTablet could end with '//'
      String tabletArgs = OzoneFSUtils.addTrailingSlashIfNeeded(
              metadataManager.getOzoneTablet(databaseName, tableName, partitionName, tabletName));

      // First, find tablet in TableCache
      listStatusFindTabletInTableCache(cacheIter, tabletArgs, startCacheTablet,
              cacheTabletKeyMap, deletedTabletSet);
      // Then, find tablet in DB
      String seekTabletInDb =
          metadataManager.getOzoneTablet(databaseName, tableName, partitionName, startTablet);
      TableIterator<String, ? extends Table.KeyValue<String, OmTabletInfo>>
          iterator = tabletTable.iterator();
      iterator.seek(seekTabletInDb);
      int countEntries = 0;
      if (iterator.hasNext()) {
        if (iterator.key().equals(tabletArgs)) {
          // Skip the tabletKey itself, since we are listing inside the directory
          iterator.next();
        }
        // Iterate through seek results
        while (iterator.hasNext() && numEntries - countEntries > 0) {
          String entryInDb = iterator.key();
          OmTabletInfo omTabletInfo = iterator.value().getValue();
          if (entryInDb.startsWith(tabletArgs)) {
            String entryTabletName = omTabletInfo.getTabletName();
              // for recursive list all the entries
              if (!deletedTabletSet.contains(entryInDb)) {
                cacheTabletKeyMap.put(entryInDb, new OzoneTabletStatus(omTabletInfo,
                    scmBlockSize));
                countEntries++;
              }
              iterator.next();
            }
        }
      }

      countEntries = 0;
      // Convert results in cacheTabletMap to List
      for (OzoneTabletStatus tabletStatus : cacheTabletKeyMap.values()) {
        // No need to check if a tablet is deleted or not here, this is handled
        // when adding entries to cacheTabletKeyMap from DB.
        tabletStatusList.add(tabletStatus);
        countEntries++;
        if (countEntries >= numEntries) {
          break;
        }
      }
      // Clean up temp map and set
      cacheTabletKeyMap.clear();
      deletedTabletSet.clear();
    } finally {
      metadataManager.getLock().releaseReadLock(PARTITION_LOCK, databaseName,
          tableName, partitionName);
    }

    List<OmTabletInfo> tabletInfoList = new ArrayList<>(tabletStatusList.size());
    for (OzoneTabletStatus tabletStatus : tabletStatusList) {
      tabletInfoList.add(tabletStatus.getTabletInfo());
    }
    refreshPipeline(tabletInfoList);

    if (args.getSortDatanodes()) {
      sortDatanodes(clientAddress, tabletInfoList.toArray(new OmTabletInfo[0]));
    }

    return tabletStatusList;
  }

/**
 * Ozone api to creates an output stream for a tablet.
 *
 * @param args        Tablet args
 * @param isOverWrite if true existing tablet at the location will be
 *                    overwritten
 * @throws OMException if given tablet is a directory
 *                     if tablet exists and isOverwrite flag is false
 *                     if an ancestor exists as a tablet
 *                     if partition does not exist
 *                     if table does not exist
 * @throws IOException if there is error in the db
 *                     invalid arguments
 */
  @Override
  public OpenTabletSession createTablet(OmTabletArgs args, boolean isOverWrite) throws IOException {
    Preconditions.checkNotNull(args, "Tablet args can not be null");
    String databaseName = args.getDatabaseName();
    String tableName = args.getTableName();
    String partitionName = args.getPartitionName();
    String tabletName = args.getTabletName();
    OpenTabletSession tabletSession = null;

    metadataManager.getLock().acquireWriteLock(PARTITION_LOCK, databaseName,
            tableName, partitionName);
    try {

      try {
        OzoneTabletStatus tabletStatus = getTabletStatus(args);
        if (tabletStatus != null) {
         if (!isOverWrite) {
           throw new OMException("Tablet " + tabletName + " already exists",
                   ResultCodes.TABLET_ALREADY_EXISTS);
         } else {
           // TODO: Optimize call to openTablet as tabletInfo is already available in the
           // tablet status. We can avoid some operations in openTablet call.
           tabletSession = openTablet(args);
         }
        }
      } catch (OMException ex) {
        if (ex.getResult() != TABLET_NOT_FOUND) {
          throw ex;
        }
      }

    } finally {
      metadataManager.getLock().releaseWriteLock(PARTITION_LOCK, databaseName,
              tableName, partitionName);
    }
    return tabletSession;
  }

  @VisibleForTesting
  public void sortDatanodes(String clientMachine, OmTabletInfo... tabletInfos) {
    if (tabletInfos != null && clientMachine != null && !clientMachine.isEmpty()) {
      Map<Set<String>, List<DatanodeDetails>> sortedPipelines = new HashMap<>();
      for (OmTabletInfo tabletInfo : tabletInfos) {
        OmTabletLocationInfoGroup tablet = tabletInfo.getLatestVersionLocations();
        if (tablet== null) {
          LOG.warn("No location for tablet {}", tabletInfo);
          continue;
        }
        for (OmTabletLocationInfo k : tablet.getLocationList()) {
          Pipeline pipeline = k.getPipeline();
          List<DatanodeDetails> nodes = pipeline.getNodes();
          List<String> uuidList = toNodeUuid(nodes);
          Set<String> uuidSet = new HashSet<>(uuidList);
          List<DatanodeDetails> sortedNodes = sortedPipelines.get(uuidSet);
          if (sortedNodes == null) {
            if (nodes.isEmpty()) {
              LOG.warn("No datanodes in pipeline {}", pipeline.getId());
              continue;
            }
            sortedNodes = sortDatanodes(clientMachine, nodes, tabletInfo,
                uuidList);
            if (sortedNodes != null) {
              sortedPipelines.put(uuidSet, sortedNodes);
            }
          } else if (LOG.isDebugEnabled()) {
            LOG.debug("Found sorted datanodes for pipeline {} and client {} "
                + "in cache", pipeline.getId(), clientMachine);
          }
          pipeline.setNodesInOrder(sortedNodes);
        }
      }
    }
  }

  private List<DatanodeDetails> sortDatanodes(String clientMachine,
      List<DatanodeDetails> nodes, OmTabletInfo tabletInfo, List<String> nodeList) {
    List<DatanodeDetails> sortedNodes = null;
    try {
      sortedNodes = scmClient.getBlockClient()
          .sortDatanodes(nodeList, clientMachine);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Sorted datanodes {} for client {}, result: {}", nodes,
            clientMachine, sortedNodes);
      }
    } catch (IOException e) {
      LOG.warn("Unable to sort datanodes based on distance to client, "
          + " database={}, table={}, partition={}, tablet={}, client={}, datanodes={}, "
          + " exception={}",
          tabletInfo.getDatabaseName(), tabletInfo.getTableName(),
          tabletInfo.getPartitionName(), tabletInfo.getTabletName(), clientMachine,
          nodeList, e.getMessage());
    }
    return sortedNodes;
  }

  private static List<String> toNodeUuid(Collection<DatanodeDetails> nodes) {
    List<String> nodeSet = new ArrayList<>(nodes.size());
    for (DatanodeDetails node : nodes) {
      nodeSet.add(node.getUuidString());
    }
    return nodeSet;
  }
}
