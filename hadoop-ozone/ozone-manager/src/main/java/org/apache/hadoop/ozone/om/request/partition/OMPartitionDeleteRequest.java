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

package org.apache.hadoop.ozone.om.request.partition;

import com.google.common.base.Optional;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.hm.HmDatabaseArgs;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmTableInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.partition.OMPartitionDeleteResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeletePartitionRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeletePartitionResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.PARTITION_NOT_FOUND;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.DATABASE_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.PARTITION_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.TABLE_LOCK;

/**
 * Handles DeletePartition Request.
 */
public class OMPartitionDeleteRequest extends OMClientRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMPartitionDeleteRequest.class);

  public OMPartitionDeleteRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {
    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumPartitionDeletes();
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    OMRequest omRequest = getOmRequest();
    DeletePartitionRequest deletePartitionRequest =
        omRequest.getDeletePartitionRequest();
    String databaseName = deletePartitionRequest.getDatabaseName();
    String tableName = deletePartitionRequest.getTableName();
    String partitionName = deletePartitionRequest.getPartitionName();

    // Generate end user response
    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());

    AuditLogger auditLogger = ozoneManager.getAuditLogger();
    Map<String, String> auditMap = buildDatabaseAuditMap(databaseName);
    auditMap.put(OzoneConsts.TABLE, tableName);
    auditMap.put(OzoneConsts.PARTITION, partitionName);

    OzoneManagerProtocolProtos.UserInfo userInfo = getOmRequest().getUserInfo();
    IOException exception = null;

    boolean acquiredPartitionLock = false;
    boolean acquiredDatabaseLock = false;
    boolean acquiredTableLock = false;
    boolean success = true;
    OMClientResponse omClientResponse = null;
    try {
      // check Acl
      if (ozoneManager.getAclsEnabled()) {
        /*checkAcls(ozoneManager, OzoneObj.ResourceType.TABLE,
            OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.DELETE,
            databaseName, tableName, null);*/
      }

      // acquire lock
      // acquire lock
      acquiredDatabaseLock =
              omMetadataManager.getLock().acquireReadLock(DATABASE_LOCK, databaseName);
      acquiredTableLock =
              omMetadataManager.getLock().acquireReadLock(TABLE_LOCK,
                      databaseName, tableName);
      acquiredPartitionLock =
          omMetadataManager.getLock().acquireWriteLock(PARTITION_LOCK, databaseName,
                  tableName, partitionName);

      // No need to check table exists here, as table cannot be created
      // with out table creation. Check if partition exists
      String partitionKey = omMetadataManager.getPartitionKey(databaseName, tableName, partitionName);
      if (!omMetadataManager.getPartitionTable().isExist(partitionKey)) {
        LOG.debug("partition: {} not found ", partitionName);
        throw new OMException("Partition already exist", PARTITION_NOT_FOUND);
      }

      //Check if partition is empty
      if (!omMetadataManager.isPartitionEmpty(databaseName, tableName, partitionName)) {
        LOG.debug("partition: {} is not empty ", partitionName);
        throw new OMException("Partition is not empty",
            OMException.ResultCodes.PARTITION_NOT_EMPTY);
      }
      omMetrics.decNumPartitions();

      // Update partition cache.
      omMetadataManager.getPartitionTable().addCacheEntry(
          new CacheKey<>(partitionKey),
          new CacheValue<>(Optional.absent(), transactionLogIndex));

      omResponse.setDeletePartitionResponse(
          DeletePartitionResponse.newBuilder().build());

      // Get  omTableInfo
      String tableKey = omMetadataManager.getMetaTableKey(databaseName, tableName);
      OmTableInfo omTableInfo =
              omMetadataManager.getMetaTable().getReadCopy(tableKey);
      if (omTableInfo == null) {
        throw new OMException("Table " + tableName + " is not found",
                OMException.ResultCodes.TABLE_NOT_FOUND);
      }

      // Add to double buffer.
      omClientResponse = new OMPartitionDeleteResponse(omResponse.build(),
          databaseName, tableName, partitionName, omTableInfo.copyObject());
    } catch (IOException ex) {
      success = false;
      exception = ex;
      omClientResponse = new OMPartitionDeleteResponse(
          createErrorOMResponse(omResponse, exception));
    } finally {
      addResponseToDoubleBuffer(transactionLogIndex, omClientResponse,
          ozoneManagerDoubleBufferHelper);
      if (acquiredPartitionLock) {
        omMetadataManager.getLock().releaseWriteLock(PARTITION_LOCK, databaseName,
                tableName, partitionName);
      }
      if (acquiredTableLock) {
        omMetadataManager.getLock().releaseReadLock(TABLE_LOCK, databaseName,
                tableName);
      }
      if (acquiredDatabaseLock) {
        omMetadataManager.getLock().releaseReadLock(DATABASE_LOCK, databaseName);
      }
    }

    // Performing audit logging outside of the lock.
    auditLog(auditLogger, buildAuditMessage(OMAction.DELETE_PARTITION,
        auditMap, exception, userInfo));

    // return response.
    if (success) {
      LOG.debug("Deleted partition:{} in table:{}.{}", partitionName, databaseName, tableName);
      return omClientResponse;
    } else {
      omMetrics.incNumPartitionDeleteFails();
      LOG.error("Delete partition failed for partition:{} in table:{}.{}", partitionName, databaseName, tableName,
          databaseName, exception);
      return omClientResponse;
    }
  }
}
