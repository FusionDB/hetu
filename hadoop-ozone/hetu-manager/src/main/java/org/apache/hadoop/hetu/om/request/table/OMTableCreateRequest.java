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

package org.apache.hadoop.hetu.om.request.table;

import com.google.common.base.Optional;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.hm.OmDatabaseArgs;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.hetu.om.OMMetrics;
import org.apache.hadoop.hetu.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmTableInfo;
import org.apache.hadoop.hetu.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.hetu.om.request.OMClientRequest;
import org.apache.hadoop.hetu.om.request.util.OmResponseUtil;
import org.apache.hadoop.hetu.om.response.OMClientResponse;
import org.apache.hadoop.hetu.om.response.table.OMTableCreateResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TableInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateTableRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateTableResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.DATABASE_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.TABLE_ALREADY_EXISTS;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.DATABASE_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.TABLE_LOCK;

/**
 * Handles CreateTable Request.
 */
public class OMTableCreateRequest extends OMClientRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMTableCreateRequest.class);

  public OMTableCreateRequest(OMRequest omRequest) {
    super(omRequest);
  }
  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {

    // Get original request.
    CreateTableRequest createTableRequest =
        getOmRequest().getCreateTableRequest();
    TableInfo tableInfo = createTableRequest.getTableInfo();
    // Verify resource name
    OmUtils.validateTableName(tableInfo.getTableName());

    // Create new Table request with new table info.
    CreateTableRequest.Builder newCreateTableRequest =
        createTableRequest.toBuilder();

    TableInfo.Builder newTableInfo = tableInfo.toBuilder();

    // Set creation time & modification time.
    long initialTime = Time.now();
    newTableInfo.setCreationTime(initialTime)
        .setModificationTime(initialTime);

    newCreateTableRequest.setTableInfo(newTableInfo.build());

    return getOmRequest().toBuilder().setUserInfo(getUserInfo())
       .setCreateTableRequest(newCreateTableRequest.build()).build();
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {
    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumTableCreates();

    OMMetadataManager metadataManager = ozoneManager.getMetadataManager();

    CreateTableRequest createTableRequest = getOmRequest()
        .getCreateTableRequest();
    TableInfo tableInfo = createTableRequest.getTableInfo();

    String databaseName = tableInfo.getDatabaseName();
    String tableName = tableInfo.getTableName();

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());
    OmTableInfo omTableInfo = OmTableInfo.getFromProtobuf(tableInfo);

    AuditLogger auditLogger = ozoneManager.getAuditLogger();
    OzoneManagerProtocolProtos.UserInfo userInfo = getOmRequest().getUserInfo();

    String databaseKey = metadataManager.getDatabaseKey(databaseName);
    String tableKey = metadataManager.getMetaTableKey(databaseName, tableName);
    IOException exception = null;
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

      acquiredDatabaseLock =
          metadataManager.getLock().acquireReadLock(DATABASE_LOCK, databaseName);
      acquiredTableLock = metadataManager.getLock().acquireWriteLock(
          TABLE_LOCK, databaseName, tableName);

      OmDatabaseArgs omDatabaseArgs =
          metadataManager.getDatabaseTable().getReadCopy(databaseKey);
      //Check if the database exists
      if (omDatabaseArgs == null) {
        LOG.debug("database: {} not found ", databaseName);
        throw new OMException("Databsae doesn't exist", DATABASE_NOT_FOUND);
      }

      //Check if database already exists
      if (metadataManager.getMetaTable().isExist(tableKey)) {
        LOG.debug("table: {} already exists ", tableName);
        throw new OMException("Table already exist", TABLE_ALREADY_EXISTS);
      }

      //Check quotaInBytes to update
      checkQuotaBytesValid(metadataManager, omDatabaseArgs, omTableInfo,
          databaseKey);

      // TODO: check schema, partitions, numReplicas

      // Add objectID and updateID
      omTableInfo.setObjectID(
          ozoneManager.getObjectIdFromTxId(transactionLogIndex));
      omTableInfo.setUpdateID(transactionLogIndex,
          ozoneManager.isRatisEnabled());

      // update used namespace for database
      omDatabaseArgs.incrUsedNamespace(1L);

      // Update table cache.
      metadataManager.getDatabaseTable().addCacheEntry(new CacheKey<>(databaseKey),
          new CacheValue<>(Optional.of(omDatabaseArgs), transactionLogIndex));
      metadataManager.getMetaTable().addCacheEntry(new CacheKey<>(tableKey),
          new CacheValue<>(Optional.of(omTableInfo), transactionLogIndex));

      omResponse.setCreateTableResponse(
          CreateTableResponse.newBuilder().build());
      omClientResponse = new OMTableCreateResponse(omResponse.build(),
          omTableInfo, omDatabaseArgs.copyObject());
    } catch (IOException ex) {
      exception = ex;
      omClientResponse = new OMTableCreateResponse(
          createErrorOMResponse(omResponse, exception));
    } finally {
      addResponseToDoubleBuffer(transactionLogIndex, omClientResponse,
          ozoneManagerDoubleBufferHelper);
      if (acquiredTableLock) {
        metadataManager.getLock().releaseWriteLock(TABLE_LOCK, databaseName,
            tableName);
      }
      if (acquiredDatabaseLock) {
        metadataManager.getLock().releaseReadLock(DATABASE_LOCK, databaseName);
      }
    }

    // Performing audit logging outside of the lock.
    auditLog(auditLogger, buildAuditMessage(OMAction.CREATE_TABLE,
        omTableInfo.toAuditMap(), exception, userInfo));

    // return response.
    if (exception == null) {
      LOG.debug("created table: {} in database: {}", tableName, databaseName);
      omMetrics.incNumTables();
      return omClientResponse;
    } else {
      omMetrics.incNumTableCreateFails();
      LOG.error("Table creation failed for table:{} in database:{}",
          tableName, databaseName, exception);
      return omClientResponse;
    }
  }

  public boolean checkQuotaBytesValid(OMMetadataManager metadataManager,
                                      OmDatabaseArgs omDatabaseArgs, OmTableInfo omTableInfo, String databaseKey)
      throws IOException {
    long usedCapacityInBytes = omTableInfo.getUsedInBytes();
    long databaseQuotaInBytes = omDatabaseArgs.getQuotaInBytes();

    long totalTableQuota = 0;
    if (usedCapacityInBytes > 0) {
      totalTableQuota = usedCapacityInBytes;
    } else {
      return false;
    }

    List<OmTableInfo>  tableList = metadataManager.listMetaTables(
        omDatabaseArgs.getName(), null, null, Integer.MAX_VALUE);
    for(OmTableInfo tableInfo : tableList) {
      long nextUsedCapacityInBytes = tableInfo.getUsedInBytes();
      if(nextUsedCapacityInBytes > OzoneConsts.USED_CAPACITY_IN_BYTES_RESET) {
        totalTableQuota += nextUsedCapacityInBytes;
      }
    }
    if(databaseQuotaInBytes < totalTableQuota
        && databaseQuotaInBytes != OzoneConsts.QUOTA_RESET) {
      throw new IllegalArgumentException("Total tables quota in this databse " +
          "should not be greater than database quota : the total space quota is" +
          " set to:" + totalTableQuota + ". But the database space quota is:" +
          databaseQuotaInBytes);
    }
    return true;

  }

}
