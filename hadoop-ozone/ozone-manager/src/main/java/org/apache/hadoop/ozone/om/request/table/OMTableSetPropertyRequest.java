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

package org.apache.hadoop.ozone.om.request.table;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.hetu.hm.helpers.OmDatabaseArgs;
import org.apache.hadoop.hetu.hm.meta.table.ColumnSchema;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.KeyValueUtil;
import org.apache.hadoop.ozone.om.helpers.OmTableArgs;
import org.apache.hadoop.ozone.om.helpers.OmTableInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.table.OMTableSetPropertyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TableArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetTablePropertyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetTablePropertyResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.TABLE_LOCK;

/**
 * Handle SetTableProperty Request.
 */
public class OMTableSetPropertyRequest extends OMClientRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMTableSetPropertyRequest.class);

  public OMTableSetPropertyRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  @SuppressWarnings("methodlength")
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {

    SetTablePropertyRequest setTablePropertyRequest =
        getOmRequest().getSetTablePropertyRequest();
    Preconditions.checkNotNull(setTablePropertyRequest);

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumTableUpdates();

    TableArgs tableArgs = setTablePropertyRequest.getTableArgs();
    OmTableArgs omTableArgs = OmTableArgs.getFromProtobuf(tableArgs);

    String databaseName = tableArgs.getDatabaseName();
    String tableName = tableArgs.getTableName();

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());
    OmTableInfo omTableInfo = null;

    AuditLogger auditLogger = ozoneManager.getAuditLogger();
    OzoneManagerProtocolProtos.UserInfo userInfo = getOmRequest().getUserInfo();
    IOException exception = null;
    boolean acquiredTableLock = false, success = true;
    OMClientResponse omClientResponse = null;
    try {
      // check Acl
      if (ozoneManager.getAclsEnabled()) {
//        checkAcls(ozoneManager, OzoneObj.ResourceType.TABLE,
//            OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.WRITE,
//            databaseName, tableName, null);
      }

      // acquire lock.
      acquiredTableLock =  omMetadataManager.getLock().acquireWriteLock(
          TABLE_LOCK, databaseName, tableName);

      String tableKey = omMetadataManager.getMetaTableKey(databaseName, tableName);
      OmTableInfo dbTableInfo =
          omMetadataManager.getMetaTable().get(tableKey);
      //Check if table exist
      if (dbTableInfo == null) {
        LOG.debug("table: {} not found ", tableName);
        throw new OMException("Table doesn't exist",
            OMException.ResultCodes.TABLE_NOT_FOUND);
      }

      OmTableInfo.Builder tableInfoBuilder = OmTableInfo.newBuilder();
      tableInfoBuilder.setDatabaseName(dbTableInfo.getDatabaseName())
          .setTableName(dbTableInfo.getTableName())
          .setObjectID(dbTableInfo.getObjectID())
          .setUpdateID(transactionLogIndex);
      tableInfoBuilder.addAllMetadata(KeyValueUtil
          .getFromProtobuf(tableArgs.getMetadataList()));
      tableInfoBuilder.setColumns(tableArgs.getColumnsList()
              .stream()
              .map(columnSchemaProto -> ColumnSchema.fromProtobuf(columnSchemaProto))
              .collect(Collectors.toList()));
      tableInfoBuilder.setNumReplicas(tableArgs.getNumReplicas());

      //Check StorageType to update
      StorageType storageType = omTableArgs.getStorageType();
      if (storageType != null) {
        tableInfoBuilder.setStorageType(storageType);
        LOG.debug("Updating table storage type for table: {} in table: {}",
            tableName, databaseName);
      } else {
        tableInfoBuilder.setStorageType(dbTableInfo.getStorageType());
      }

      //Check Versioning to update
      Boolean versioning = omTableArgs.getIsVersionEnabled();
      if (versioning != null) {
        tableInfoBuilder.setIsVersionEnabled(versioning);
        LOG.debug("Updating table versioning for table: {} in database: {}",
            tableName, databaseName);
      } else {
        tableInfoBuilder
            .setIsVersionEnabled(dbTableInfo.getIsVersionEnabled());
      }

      //Check usedCapacityInBytes to update
      String databaseKey = omMetadataManager.getDatabaseKey(databaseName);
      OmDatabaseArgs omDatabaseArgs = omMetadataManager.getDatabaseTable()
          .get(databaseKey);
      if (checkQuotaBytesValid(omMetadataManager, omDatabaseArgs, omTableArgs,
          databaseKey)) {
        tableInfoBuilder.setUsedInBytes(omTableArgs.getUsedInBytes());
      } else {
        tableInfoBuilder.setUsedInBytes(dbTableInfo.getUsedInBytes());
      }

      tableInfoBuilder.setCreationTime(dbTableInfo.getCreationTime());

      // Set the objectID to dbTableInfo objectID, if present
      if (dbTableInfo.getObjectID() != 0) {
        tableInfoBuilder.setObjectID(dbTableInfo.getObjectID());
      }

      // Set the updateID to current transaction log index
      tableInfoBuilder.setUpdateID(transactionLogIndex);

      omTableInfo = tableInfoBuilder.build();

      // Update table cache.
      omMetadataManager.getMetaTable().addCacheEntry(
          new CacheKey<>(tableKey),
          new CacheValue<>(Optional.of(omTableInfo), transactionLogIndex));

      omResponse.setSetTablePropertyResponse(
          SetTablePropertyResponse.newBuilder().build());
      omClientResponse = new OMTableSetPropertyResponse(
          omResponse.build(), omTableInfo);
    } catch (IOException ex) {
      success = false;
      exception = ex;
      omClientResponse = new OMTableSetPropertyResponse(
          createErrorOMResponse(omResponse, exception));
    } finally {
      addResponseToDoubleBuffer(transactionLogIndex, omClientResponse,
          ozoneManagerDoubleBufferHelper);
      if (acquiredTableLock) {
        omMetadataManager.getLock().releaseWriteLock(TABLE_LOCK, databaseName,
            tableName);
      }
    }

    // Performing audit logging outside of the lock.
    auditLog(auditLogger, buildAuditMessage(OMAction.UPDATE_TABLE,
        omTableArgs.toAuditMap(), exception, userInfo));

    // return response.
    if (success) {
      LOG.debug("Setting table property for table:{} in database:{}",
          tableName, databaseName);
      return omClientResponse;
    } else {
      LOG.error("Setting table property failed for table:{} in database:{}",
          tableName, databaseName, exception);
      omMetrics.incNumTableUpdateFails();
      return omClientResponse;
    }
  }

  public boolean checkQuotaBytesValid(OMMetadataManager metadataManager,
                                      OmDatabaseArgs omDatabaseArgs, OmTableArgs omTableArgs, String databaseKey)
      throws IOException {
    long usedCapacityInBytes = omTableArgs.getUsedInBytes();

    if (usedCapacityInBytes == OzoneConsts.USED_CAPACITY_IN_BYTES_RESET &&
        omDatabaseArgs.getQuotaInBytes() != OzoneConsts.QUOTA_RESET) {
      throw new OMException("Can not clear table spaceQuota because" +
          " database spaceQuota is not cleared.",
          OMException.ResultCodes.QUOTA_ERROR);
    }

    if (usedCapacityInBytes < OzoneConsts.USED_CAPACITY_IN_BYTES_RESET || usedCapacityInBytes == 0) {
      return false;
    }

    long totalTableQuota = 0;
    long databaseQuotaInBytes = omDatabaseArgs.getQuotaInBytes();

    if (usedCapacityInBytes > OzoneConsts.USED_CAPACITY_IN_BYTES_RESET) {
      totalTableQuota = usedCapacityInBytes;
    }
    List<OmTableInfo> tableList = metadataManager.listMetaTables(
        omDatabaseArgs.getName(), null, null, Integer.MAX_VALUE);
    for(OmTableInfo tableInfo : tableList) {
      long nextQuotaInBytes = tableInfo.getUsedInBytes();
      if(nextQuotaInBytes > OzoneConsts.USED_CAPACITY_IN_BYTES_RESET &&
          !omTableArgs.getTableName().equals(tableInfo.getTableName())) {
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
