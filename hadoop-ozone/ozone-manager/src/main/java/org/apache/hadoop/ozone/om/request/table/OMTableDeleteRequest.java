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
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.hetu.hm.helpers.OmDatabaseArgs;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.table.OMTableDeleteResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteTableRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteTableResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.TABLE_NOT_FOUND;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.DATABASE_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.TABLE_LOCK;

/**
 * Handles DeleteTable Request.
 */
public class OMTableDeleteRequest extends OMClientRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMTableDeleteRequest.class);

  public OMTableDeleteRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {
    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumTableDeletes();
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    OMRequest omRequest = getOmRequest();
    DeleteTableRequest deleteTableRequest =
        omRequest.getDeleteTableRequest();
    String databaseName = deleteTableRequest.getDatabaseName();
    String tableName = deleteTableRequest.getTableName();

    // Generate end user response
    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());

    AuditLogger auditLogger = ozoneManager.getAuditLogger();
    Map<String, String> auditMap = buildDatabaseAuditMap(databaseName);
    auditMap.put(OzoneConsts.TABLE, tableName);

    OzoneManagerProtocolProtos.UserInfo userInfo = getOmRequest().getUserInfo();
    IOException exception = null;

    boolean acquiredTableLock = false, acquiredDatabaseLock = false;
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
      acquiredDatabaseLock =
          omMetadataManager.getLock().acquireReadLock(DATABASE_LOCK, databaseName);
      acquiredTableLock =
          omMetadataManager.getLock().acquireWriteLock(TABLE_LOCK,
          databaseName, tableName);

      // No need to check database exists here, as table cannot be created
      // with out database creation. Check if table exists
      String tableKey = omMetadataManager.getMetaTableKey(databaseName, tableName);

      if (!omMetadataManager.getMetaTable().isExist(tableKey)) {
        LOG.debug("table: {} not found ", tableName);
        throw new OMException("Table already exist", TABLE_NOT_FOUND);
      }

      //Check if table is empty
      if (!omMetadataManager.isMetaTableEmpty(databaseName, tableName)) {
        LOG.debug("table: {} is not empty ", tableName);
        throw new OMException("Table is not empty",
            OMException.ResultCodes.TABLE_NOT_EMPTY);
      }
      omMetrics.decNumTables();

      // Update table cache.
      omMetadataManager.getMetaTable().addCacheEntry(
          new CacheKey<>(tableKey),
          new CacheValue<>(Optional.absent(), transactionLogIndex));

      omResponse.setDeleteTableResponse(
          DeleteTableResponse.newBuilder().build());

      // update used namespace for database
      String databaseKey = omMetadataManager.getDatabaseKey(databaseName);
      OmDatabaseArgs omDatabaseArgs =
          omMetadataManager.getDatabaseTable().getReadCopy(databaseKey);
      if (omDatabaseArgs == null) {
        throw new OMException("Database " + databaseName + " is not found",
            OMException.ResultCodes.DATABASE_NOT_FOUND);
      }
      omDatabaseArgs.incrUsedNamespace(-1L);
      // Update table cache.
      omMetadataManager.getDatabaseTable().addCacheEntry(
          new CacheKey<>(databaseKey),
          new CacheValue<>(Optional.of(omDatabaseArgs), transactionLogIndex));

      // Add to double buffer.
      omClientResponse = new OMTableDeleteResponse(omResponse.build(),
          databaseName, tableName, omDatabaseArgs.copyObject());
    } catch (IOException ex) {
      success = false;
      exception = ex;
      omClientResponse = new OMTableDeleteResponse(
          createErrorOMResponse(omResponse, exception));
    } finally {
      addResponseToDoubleBuffer(transactionLogIndex, omClientResponse,
          ozoneManagerDoubleBufferHelper);
      if (acquiredTableLock) {
        omMetadataManager.getLock().releaseWriteLock(TABLE_LOCK, databaseName,
            tableName);
      }
      if (acquiredDatabaseLock) {
        omMetadataManager.getLock().releaseReadLock(DATABASE_LOCK, databaseName);
      }
    }

    // Performing audit logging outside of the lock.
    auditLog(auditLogger, buildAuditMessage(OMAction.DELETE_TABLE,
        auditMap, exception, userInfo));

    // return response.
    if (success) {
      LOG.debug("Deleted table:{} in database:{}", tableName, databaseName);
      return omClientResponse;
    } else {
      omMetrics.incNumTableDeleteFails();
      LOG.error("Delete table failed for table:{} in database:{}", tableName,
          databaseName, exception);
      return omClientResponse;
    }
  }
}
