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
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.hm.OmDatabaseArgs;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.hetu.om.OMMetrics;
import org.apache.hadoop.hetu.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmPartitionInfo;
import org.apache.hadoop.ozone.om.helpers.OmTableInfo;
import org.apache.hadoop.hetu.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.hetu.om.request.OMClientRequest;
import org.apache.hadoop.hetu.om.request.util.OmResponseUtil;
import org.apache.hadoop.hetu.om.response.OMClientResponse;
import org.apache.hadoop.hetu.om.response.partition.OMPartitionCreateResponse;
import org.apache.hadoop.hetu.om.response.table.OMTableCreateResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreatePartitionRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreatePartitionResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PartitionInfo;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.DATABASE_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.PARTITION_ALREADY_EXISTS;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.TABLE_NOT_FOUND;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.DATABASE_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.PARTITION_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.TABLE_LOCK;

/**
 * Handles CreateTable Request.
 */
public class OMPartitionCreateRequest extends OMClientRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMPartitionCreateRequest.class);

  public OMPartitionCreateRequest(OMRequest omRequest) {
    super(omRequest);
  }
  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {

    // Get original request.
    CreatePartitionRequest createPartitionRequest =
        getOmRequest().getCreatePartitionRequest();
    PartitionInfo partitionInfo = createPartitionRequest.getPartitionInfo();
    // Verify resource name
    OmUtils.validatePartitionName(partitionInfo.getPartitionName());

    // Create new partition request with new partition info.
    CreatePartitionRequest.Builder newCreatePartitionRequest =
        createPartitionRequest.toBuilder();

    PartitionInfo.Builder newPartitionInfo = partitionInfo.toBuilder();

    // Set creation time & modification time.
    long initialTime = Time.now();
    newPartitionInfo.setCreationTime(initialTime)
        .setModificationTime(initialTime);

    newCreatePartitionRequest.setPartitionInfo(newPartitionInfo.build());

    return getOmRequest().toBuilder().setUserInfo(getUserInfo())
       .setCreatePartitionRequest(newCreatePartitionRequest.build()).build();
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {
    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumPartitionCreates();

    OMMetadataManager metadataManager = ozoneManager.getMetadataManager();

    CreatePartitionRequest createPartitionRequest = getOmRequest()
        .getCreatePartitionRequest();
    PartitionInfo partitionInfo = createPartitionRequest.getPartitionInfo();

    String databaseName = partitionInfo.getDatabaseName();
    String tableName = partitionInfo.getTableName();
    String partitionName = partitionInfo.getPartitionName();

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());
    OmPartitionInfo omPartitionInfo = OmPartitionInfo.getFromProtobuf(partitionInfo);

    AuditLogger auditLogger = ozoneManager.getAuditLogger();
    OzoneManagerProtocolProtos.UserInfo userInfo = getOmRequest().getUserInfo();

    String databaseKey = metadataManager.getDatabaseKey(databaseName);
    String tableKey = metadataManager.getMetaTableKey(databaseName, tableName);
    String partitionKey = metadataManager.getPartitionKey(databaseName, tableName, partitionName);

    IOException exception = null;
    boolean acquiredPartitionLock = false;
    boolean acquiredTableLock = false;
    boolean acquiredDatabaseLock = false;
    OMClientResponse omClientResponse = null;

    try {
      // check Acl
      if (ozoneManager.getAclsEnabled()) {
//        checkAcls(ozoneManager, OzoneObj.ResourceType.TABLE,
//            OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.CREATE,
//            databaseName, tableName, null);
      }

      acquiredDatabaseLock = metadataManager.getLock().acquireWriteLock(
              DATABASE_LOCK, databaseName);
      acquiredTableLock = metadataManager.getLock().acquireWriteLock(
              TABLE_LOCK, databaseName, tableName);
      acquiredPartitionLock = metadataManager.getLock().acquireWriteLock(
              PARTITION_LOCK, databaseName, tableName, partitionName);

      OmDatabaseArgs omDatabaseArgs =
          metadataManager.getDatabaseTable().getReadCopy(databaseKey);
      //Check if the database exists
      if (omDatabaseArgs == null) {
        LOG.debug("database: {} not found ", databaseName);
        throw new OMException("Database doesn't exist", DATABASE_NOT_FOUND);
      }

      //Check if the table exists
      OmTableInfo omTableInfo = metadataManager.getMetaTable().getReadCopy(tableKey);
      if (!metadataManager.getMetaTable().isExist(tableKey)) {
        LOG.debug("table: {}.{}  not found ", databaseName, tableName);
        throw new OMException("Table doesn't exist", TABLE_NOT_FOUND);
      }

      // Check if partition already exists
      if (metadataManager.getPartitionTable().isExist(partitionKey)) {
        LOG.debug("partition: {}.{}_{} already exists ", databaseName, tableName, partitionName);
        throw new OMException("Partition already exist", PARTITION_ALREADY_EXISTS);
      }

      // Add objectID and updateID
      omTableInfo.setObjectID(
          ozoneManager.getObjectIdFromTxId(transactionLogIndex));
      omTableInfo.setUpdateID(transactionLogIndex,
          ozoneManager.isRatisEnabled());

      // Update partition cache.
      metadataManager.getPartitionTable().addCacheEntry(new CacheKey<>(partitionKey),
              new CacheValue<>(Optional.of(omPartitionInfo), transactionLogIndex));

      omResponse.setCreatePartitionResponse(
          CreatePartitionResponse.newBuilder().build());
      omClientResponse = new OMPartitionCreateResponse(omResponse.build(),
          omPartitionInfo, omTableInfo, omDatabaseArgs.copyObject());
    } catch (IOException ex) {
      exception = ex;
      omClientResponse = new OMTableCreateResponse(
          createErrorOMResponse(omResponse, exception));
    } finally {
      addResponseToDoubleBuffer(transactionLogIndex, omClientResponse,
          ozoneManagerDoubleBufferHelper);
      if (acquiredPartitionLock) {
        metadataManager.getLock().releaseWriteLock(PARTITION_LOCK, databaseName,
           tableName, partitionName);
      }
      if (acquiredTableLock) {
        metadataManager.getLock().releaseWriteLock(TABLE_LOCK, databaseName,
                tableName);
      }
      if (acquiredDatabaseLock) {
        metadataManager.getLock().releaseWriteLock(DATABASE_LOCK, databaseName);
      }
    }

    // Performing audit logging outside of the lock.
    auditLog(auditLogger, buildAuditMessage(OMAction.CREATE_PARTITION,
        omPartitionInfo.toAuditMap(), exception, userInfo));

    // return response.
    if (exception == null) {
      LOG.debug("created partition: {} in table: {}.{}", partitionName, databaseName, tableName);
      omMetrics.incNumPartitions();
      return omClientResponse;
    } else {
      omMetrics.incNumPartitionCreateFails();
      LOG.error("Partition creation failed for partition: {} in table: {}.{}",
          partitionName, databaseName, tableName, exception);
      return omClientResponse;
    }
  }

}
