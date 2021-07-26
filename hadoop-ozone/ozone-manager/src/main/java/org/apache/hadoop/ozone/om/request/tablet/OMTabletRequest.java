/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.request.tablet;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.hm.HmDatabaseArgs;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.PrefixManager;
import org.apache.hadoop.ozone.om.ScmClient;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketEncryptionKeyInfo;
import org.apache.hadoop.ozone.om.helpers.KeyValueUtil;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmPartitionInfo;
import org.apache.hadoop.ozone.om.helpers.OmTableInfo;
import org.apache.hadoop.ozone.om.helpers.OmTabletInfo;
import org.apache.hadoop.ozone.om.helpers.OmTabletLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmPrefixInfo;
import org.apache.hadoop.ozone.om.helpers.OmTabletLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OzoneAclUtil;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TabletInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TabletArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocolPB.OMPBHelper;
import org.apache.hadoop.ozone.security.OzoneBlockTokenSecretManager;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockTokenSecretProto.AccessModeProto.READ;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockTokenSecretProto.AccessModeProto.WRITE;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.BUCKET_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.DATABASE_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.PARTITION_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.TABLE_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.VOLUME_NOT_FOUND;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;
import static org.apache.hadoop.util.Time.monotonicNow;

/**
 * Interface for tablet write requests.
 */
public abstract class OMTabletRequest extends OMClientRequest {

  private static final Logger LOG = LoggerFactory.getLogger(OMTabletRequest.class);

  public OMTabletRequest(OMRequest omRequest) {
    super(omRequest);
  }

  /**
   * This methods avoids multiple rpc calls to SCM by allocating multiple blocks
   * in one rpc call.
   * @throws IOException
   */
  @SuppressWarnings("parameternumber")
  protected List< OmTabletLocationInfo > allocateBlock(ScmClient scmClient,
                                                       OzoneBlockTokenSecretManager secretManager,
                                                       HddsProtos.ReplicationType replicationType,
                                                       HddsProtos.ReplicationFactor replicationFactor,
                                                       ExcludeList excludeList, long requestedSize, long scmBlockSize,
                                                       int preallocateBlocksMax, boolean grpcBlockTokenEnabled, String omID)
          throws IOException {

    int numBlocks = Math.min((int) ((requestedSize - 1) / scmBlockSize + 1),
            preallocateBlocksMax);

    List<OmTabletLocationInfo> locationInfos = new ArrayList<>(numBlocks);
    String remoteUser = getRemoteUser().getShortUserName();
    List<AllocatedBlock> allocatedBlocks;
    try {
      allocatedBlocks = scmClient.getBlockClient()
              .allocateBlock(scmBlockSize, numBlocks, replicationType,
                      replicationFactor, omID, excludeList);
    } catch (SCMException ex) {
      if (ex.getResult()
              .equals(SCMException.ResultCodes.SAFE_MODE_EXCEPTION)) {
        throw new OMException(ex.getMessage(),
                OMException.ResultCodes.SCM_IN_SAFE_MODE);
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
        builder.setToken(secretManager.generateToken(remoteUser, blockID,
                EnumSet.of(READ, WRITE), scmBlockSize));
      }
      locationInfos.add(builder.build());
    }
    return locationInfos;
  }

  /* Optimize ugi lookup for RPC operations to avoid a trip through
   * UGI.getCurrentUser which is synch'ed.
   */
  private UserGroupInformation getRemoteUser() throws IOException {
    UserGroupInformation ugi = Server.getRemoteUser();
    return (ugi != null) ? ugi : UserGroupInformation.getCurrentUser();
  }

  /**
   * Validate table and database exists or not.
   * @param omMetadataManager
   * @param databaseName
   * @param tableName
   * @throws IOException
   */
  public void validatePartitionAndTableAndDatabase(OMMetadataManager omMetadataManager,
      String databaseName, String tableName, String partitionName)
      throws IOException {
    String partitionKey = omMetadataManager.getPartitionKey(databaseName, tableName, partitionName);
    // Check if partition exists
    if (!omMetadataManager.getPartitionTable().isExist(partitionKey)) {
      String databaseKey = omMetadataManager.getDatabaseKey(databaseName);
      if (!omMetadataManager.getDatabaseTable().isExist(databaseKey)) {
        throw new OMException("Database not found " + databaseName,
           DATABASE_NOT_FOUND);
      }

      String tableKey = omMetadataManager.getMetaTableKey(databaseName, tableName);
      // If the table also does not exist, we should throw table not found
      // exception
      if (!omMetadataManager.getMetaTable().isExist(tableKey)) {
        throw new OMException("Table not found " + databaseName + "." + tableName,
                TABLE_NOT_FOUND);
      }


      // if the database exists but table does not exist, throw bucket not found
      // exception
      throw new OMException("Partition not found " + partitionName, PARTITION_NOT_FOUND);
    }
  }

  // For keys batch delete and rename only
  protected String getDatabaseOwner(OMMetadataManager omMetadataManager,
      String databaseName) throws IOException {
    String databaseKey = omMetadataManager.getDatabaseKey(databaseName);
    HmDatabaseArgs databaseArgs =
        omMetadataManager.getDatabaseTable().get(databaseKey);
    if (databaseArgs == null) {
      throw new OMException("Database not found " + databaseName,
          DATABASE_NOT_FOUND);
    }
    return databaseArgs.getOwnerName();
  }

  /**
   * Create TabletInfo object.
   * @return TabletInfo
   */
  @SuppressWarnings("parameterNumber")
  protected OmTabletInfo createTabletInfo(@Nonnull TabletArgs tabletArgs,
      @Nonnull List<OmTabletLocationInfo> locations,
      @Nonnull HddsProtos.ReplicationFactor factor,
      @Nonnull HddsProtos.ReplicationType type, long size,
      @Nonnull OmPartitionInfo partitionInfo,
      long transactionLogIndex, long objectID) {
    return new OmTabletInfo.Builder()
        .setDatabaseName(tabletArgs.getDatabaseName())
        .setTableName(tabletArgs.getTableName())
        .setTabletName(tabletArgs.getTabletName())
        .setPartitionName(tabletArgs.getPartitionName())
        .setOmTabletLocationInfos(Collections.singletonList(
            new OmTabletLocationInfoGroup(0, locations)))
        .setCreationTime(tabletArgs.getModificationTime())
        .setModificationTime(tabletArgs.getModificationTime())
        .setDataSize(size)
        .setReplicationType(type)
        .setReplicationFactor(factor)
        .addAllMetadata(KeyValueUtil.getFromProtobuf(tabletArgs.getMetadataList()))
        .setObjectID(objectID)
        .setUpdateID(transactionLogIndex)
        .build();
  }

  /**
   * Prepare OmTabletInfo which will be persisted to openTabletTable.
   * @return OmTabletInfo
   * @throws IOException
   */
  @SuppressWarnings("parameternumber")
  protected OmTabletInfo prepareTabletInfo(
          @Nonnull OMMetadataManager omMetadataManager,
          @Nonnull TabletArgs tabletArgs, OmTabletInfo dbTabletInfo, long size,
          @Nonnull List<OmTabletLocationInfo> locations,
          @Nullable OmPartitionInfo omPartitionInfo,
          long transactionLogIndex,
          @Nonnull long objectID,
          boolean isRatisEnabled)
          throws IOException {
    if (tabletArgs.getIsMultipartTablet()) {
       LOG.warn("Not support multipart tablet");
      //TODO args.getMetadata
    }
    if (dbTabletInfo != null) {
      // TODO: Need to be fixed, as when key already exists, we are
      //  appending new blocks to existing key.
      // The key already exist, the new blocks will be added as new version
      // when locations.size = 0, the new version will have identical blocks
      // as its previous version
      dbTabletInfo.addNewVersion(locations, false);
      dbTabletInfo.setDataSize(size + dbTabletInfo.getDataSize());
      // The modification time is set in preExecute. Use the same
      // modification time.
      dbTabletInfo.setModificationTime(tabletArgs.getModificationTime());
      dbTabletInfo.setUpdateID(transactionLogIndex, isRatisEnabled);
      return dbTabletInfo;
    }

    // the tablet does not exist, create a new object.
    // Blocks will be appended as version 0.
    return createTabletInfo(tabletArgs, locations, tabletArgs.getFactor(),
            tabletArgs.getType(), tabletArgs.getDataSize(),
            omPartitionInfo, transactionLogIndex, objectID);
  }

  /**
   * Check database quota.
   */
  protected void checkTableQuotaInDatabase(HmDatabaseArgs hmDatabaseArgs, OmTableInfo omTableInfo,
                                             long allocatedNamespace) throws IOException {
    if (omTableInfo.getUsedCapacityInBytes() > OzoneConsts.QUOTA_RESET) {
      long usedNamespace = omTableInfo.getUsedCapacityInBytes();
      long quotaInNamespace = hmDatabaseArgs.getQuotaInNamespace();
      long toUseNamespaceInTotal = usedNamespace + allocatedNamespace;
      if (quotaInNamespace < toUseNamespaceInTotal) {
        throw new OMException("The database quota of Table:"
                + omTableInfo.getTableName() + " exceeded: quotaInNamespace: "
                + quotaInNamespace + " but database consumed: "
                + toUseNamespaceInTotal + ".",
                OMException.ResultCodes.QUOTA_EXCEEDED);
      }
    }
  }

    /**
     * Check directory exists. If exists return true, else false.
     * @param databaseName
     * @param tableName
     * @param tabletName
     * @param omMetadataManager
     * @throws IOException
     */
  protected boolean checkDirectoryAlreadyExists(String databaseName,
      String tableName, String tabletName, String partitionName, OMMetadataManager omMetadataManager)
      throws IOException {
    if (omMetadataManager.getTabletTable().isExist(
        omMetadataManager.getOzoneDirTablet(databaseName, tableName,
            partitionName, tabletName))) {
      return true;
    }
    return false;
  }

  /**
   * @return the number of bytes used by blocks pointed to by {@code omKeyInfo}.
   */
  protected static long sumBlockLengths(OmTabletInfo omTabletInfo) {
    long bytesUsed = 0;
    int keyFactor = omTabletInfo.getFactor().getNumber();
    OmTabletLocationInfoGroup keyLocationGroup =
        omTabletInfo.getLatestVersionLocations();

    for(OmTabletLocationInfo locationInfo: keyLocationGroup.getLocationList()) {
      bytesUsed += locationInfo.getLength() * keyFactor;
    }

    return bytesUsed;
  }

  /**
   * Return bucket info for the specified bucket.
   * @param omMetadataManager
   * @param databaseName
   * @param tableName
   * @return OmPartitionInfo
   * @throws IOException
   */
  protected OmPartitionInfo getPartitionInfo(OMMetadataManager omMetadataManager,
            String databaseName, String tableName, String partitionName) {
    return omMetadataManager.getPartitionTable().getCacheValue(
        new CacheKey<>(omMetadataManager.getPartitionKey(databaseName, tableName, partitionName)))
        .getCacheValue();
  }

  /**
   * Return bucket info for the specified bucket.
   * @param omMetadataManager
   * @param databaseName
   * @param tableName
   * @return OmTableInfo
   * @throws IOException
   */
  protected OmTableInfo getTableInfo(OMMetadataManager omMetadataManager,
                                             String databaseName, String tableName) {
    return omMetadataManager.getMetaTable().getCacheValue(
            new CacheKey<>(omMetadataManager.getMetaTableKey(databaseName, tableName)))
            .getCacheValue();
  }

  /**
   * Return bucket info for the specified bucket.
   * @param omMetadataManager
   * @param databaseName
   * @return HmDatabaseArgs
   * @throws IOException
   */
  protected HmDatabaseArgs getDatabaseInfo(OMMetadataManager omMetadataManager,
                                             String databaseName) {
    return omMetadataManager.getDatabaseTable().getCacheValue(
            new CacheKey<>(omMetadataManager.getDatabaseKey(databaseName)))
            .getCacheValue();
  }
}
