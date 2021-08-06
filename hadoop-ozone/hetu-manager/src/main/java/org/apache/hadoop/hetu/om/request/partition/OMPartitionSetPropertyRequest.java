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

package org.apache.hadoop.hetu.om.request.partition;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.hm.OmDatabaseArgs;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.hetu.om.OMMetrics;
import org.apache.hadoop.hetu.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.KeyValueUtil;
import org.apache.hadoop.ozone.om.helpers.OmPartitionArgs;
import org.apache.hadoop.ozone.om.helpers.OmPartitionInfo;
import org.apache.hadoop.ozone.om.helpers.OmTableInfo;
import org.apache.hadoop.hetu.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.hetu.om.request.OMClientRequest;
import org.apache.hadoop.hetu.om.request.util.OmResponseUtil;
import org.apache.hadoop.hetu.om.response.OMClientResponse;
import org.apache.hadoop.hetu.om.response.partition.OMPartitionSetPropertyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetPartitionPropertyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetPartitionPropertyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PartitionArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.PARTITION_LOCK;

/**
 * Handle SetPartitionProperty Request.
 */
public class OMPartitionSetPropertyRequest extends OMClientRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMPartitionSetPropertyRequest.class);

  public OMPartitionSetPropertyRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  @SuppressWarnings("methodlength")
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {

    SetPartitionPropertyRequest setPartitionPropertyRequest =
        getOmRequest().getSetPartitionPropertyRequest();
    Preconditions.checkNotNull(setPartitionPropertyRequest);

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumPartitionUpdates();

    PartitionArgs partitionArgs = setPartitionPropertyRequest.getPartitionArgs();
    OmPartitionArgs omPartitionArgs = OmPartitionArgs.getFromProtobuf(partitionArgs);

    String databaseName = omPartitionArgs.getDatabaseName();
    String tableName = omPartitionArgs.getTableName();
    String partitionName = omPartitionArgs.getPartitionName();

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());
    OmPartitionInfo omPartitionInfo = null;

    AuditLogger auditLogger = ozoneManager.getAuditLogger();
    OzoneManagerProtocolProtos.UserInfo userInfo = getOmRequest().getUserInfo();
    IOException exception = null;
    boolean acquiredPartitionLock = false, success = true;
    OMClientResponse omClientResponse = null;
    try {
      // check Acl
      if (ozoneManager.getAclsEnabled()) {
//        checkAcls(ozoneManager, OzoneObj.ResourceType.PARTITION,
//            OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.WRITE,
//            databaseName, tableName, partitionName, null);
      }

      // acquire lock.
      acquiredPartitionLock =  omMetadataManager.getLock().acquireWriteLock(
              PARTITION_LOCK, databaseName, tableName, partitionName);

      String partitionKey = omMetadataManager.getPartitionKey(databaseName, tableName, partitionName);
      OmPartitionInfo dbPartitionInfo =
          omMetadataManager.getPartitionTable().get(partitionKey);
      //Check if partition exist
      if (dbPartitionInfo == null) {
        LOG.debug("partition: {} not found ", partitionName);
        throw new OMException("Partition doesn't exist",
            OMException.ResultCodes.PARTITION_NOT_FOUND);
      }

      OmPartitionInfo.Builder partitionInfoBuilder = OmPartitionInfo.newBuilder();
      partitionInfoBuilder.setDatabaseName(dbPartitionInfo.getDatabaseName())
              .setTableName(dbPartitionInfo.getTableName())
              .setPartitionName(dbPartitionInfo.getPartitionName())
              .setObjectID(dbPartitionInfo.getObjectID())
              .setUpdateID(transactionLogIndex);
      partitionInfoBuilder.addAllMetadata(KeyValueUtil.getFromProtobuf(partitionArgs.getMetadataList()));
      if (partitionArgs.hasPartitionValue()) {
        partitionInfoBuilder.setPartitionValue(partitionArgs.getPartitionValue());
      }

      // Check sizeInBytes to update
      String databaseKey = omMetadataManager.getDatabaseKey(databaseName);
      OmDatabaseArgs omDatabaseArgs = omMetadataManager.getDatabaseTable()
              .get(databaseKey);
      if (checkQuotaBytesValid(omMetadataManager, omDatabaseArgs, omPartitionArgs)) {
        partitionInfoBuilder.setSizeInBytes(omPartitionArgs.getSizeInBytes());
      } else {
        partitionInfoBuilder.setSizeInBytes(dbPartitionInfo.getSizeInBytes());
      }

      // check Rows
      if (partitionArgs.hasRows()) {
        partitionInfoBuilder.setRows(partitionArgs.getRows());
      }

      //Check StorageType to update
      StorageType storageType = omPartitionArgs.getStorageType();
      if (storageType != null) {
        partitionInfoBuilder.setStorageType(storageType);
        LOG.debug("Updating partition storage type for partition: {} in partition: {}.{}",
            partitionName, databaseName, tableName);
      } else {
        partitionInfoBuilder.setStorageType(dbPartitionInfo.getStorageType());
      }

      //Check Versioning to update
      Boolean versioning = omPartitionArgs.getIsVersionEnabled();
      if (versioning != null) {
        partitionInfoBuilder.setIsVersionEnabled(versioning);
        LOG.debug("Updating partition versioning for partition: {} in table: {}.{}",
            partitionName, databaseName, tableName);
      } else {
        partitionInfoBuilder
            .setIsVersionEnabled(dbPartitionInfo.getIsVersionEnabled());
      }

      //set time
      partitionInfoBuilder.setCreationTime(dbPartitionInfo.getCreationTime());
      partitionInfoBuilder.setModificationTime(System.currentTimeMillis());

      // Set the objectID to dbPartitionInfo objectID, if present
      if (dbPartitionInfo.getObjectID() != 0) {
        partitionInfoBuilder.setObjectID(dbPartitionInfo.getObjectID());
      }

      // Set the updateID to current transaction log index
      partitionInfoBuilder.setUpdateID(transactionLogIndex);

      omPartitionInfo = partitionInfoBuilder.build();

      // Update partition cache.
      omMetadataManager.getPartitionTable().addCacheEntry(
          new CacheKey<>(partitionKey),
          new CacheValue<>(Optional.of(omPartitionInfo), transactionLogIndex));

      omResponse.setSetPartitionPropertyResponse(
          SetPartitionPropertyResponse.newBuilder().build());
      omClientResponse = new OMPartitionSetPropertyResponse(
          omResponse.build(), omPartitionInfo);
    } catch (IOException ex) {
      success = false;
      exception = ex;
      omClientResponse = new OMPartitionSetPropertyResponse(
          createErrorOMResponse(omResponse, exception));
    } finally {
      addResponseToDoubleBuffer(transactionLogIndex, omClientResponse,
          ozoneManagerDoubleBufferHelper);
      if (acquiredPartitionLock) {
        omMetadataManager.getLock().releaseWriteLock(PARTITION_LOCK, databaseName,
            tableName, partitionName);
      }
    }

    // Performing audit logging outside of the lock.
    auditLog(auditLogger, buildAuditMessage(OMAction.UPDATE_PARTITION,
        omPartitionArgs.toAuditMap(), exception, userInfo));

    // return response.
    if (success) {
      LOG.debug("Setting partition property for partition:{} in table:{}.{}",
          partitionName, databaseName, tableName);
      return omClientResponse;
    } else {
      LOG.error("Setting partition property failed for partition:{} in table:{}.{}",
          partitionName, databaseName, tableName, exception);
      omMetrics.incNumPartitionUpdateFails();
      return omClientResponse;
    }
  }

  public boolean checkQuotaBytesValid(OMMetadataManager metadataManager,
                                      OmDatabaseArgs omDatabaseArgs,
                                      OmPartitionArgs omPartitionArgs)
          throws IOException {
    long sizeInBytes = omPartitionArgs.getSizeInBytes();

    if (sizeInBytes == OzoneConsts.USED_CAPACITY_IN_BYTES_RESET &&
            omDatabaseArgs.getQuotaInBytes() != OzoneConsts.QUOTA_RESET) {
      throw new OMException("Can not clear table spaceQuota because" +
              " database spaceQuota is not cleared.",
              OMException.ResultCodes.QUOTA_ERROR);
    }

    if (sizeInBytes < OzoneConsts.USED_CAPACITY_IN_BYTES_RESET || sizeInBytes == 0) {
      return false;
    }

    long totalTableQuota = 0;
    long databaseQuotaInBytes = omDatabaseArgs.getQuotaInBytes();

    if (sizeInBytes > OzoneConsts.USED_CAPACITY_IN_BYTES_RESET) {
      totalTableQuota = sizeInBytes;
    }
    List<OmTableInfo> tableList = metadataManager.listMetaTables(
            omDatabaseArgs.getName(), null, null, Integer.MAX_VALUE);
    for(OmTableInfo tableInfo : tableList) {
      long nextQuotaInBytes = tableInfo.getUsedInBytes();
      if(nextQuotaInBytes > OzoneConsts.USED_CAPACITY_IN_BYTES_RESET) {
        totalTableQuota += nextQuotaInBytes;
      }
    }

    if(databaseQuotaInBytes < totalTableQuota &&
            databaseQuotaInBytes != OzoneConsts.QUOTA_RESET) {
      throw new IllegalArgumentException("Total tables quota in this database " +
              "should not be greater than database quota : the total space quota is" +
              " set to:" + totalTableQuota + ". But the database space quota is:" +
              databaseQuotaInBytes);
    }
    return true;
  }
}
