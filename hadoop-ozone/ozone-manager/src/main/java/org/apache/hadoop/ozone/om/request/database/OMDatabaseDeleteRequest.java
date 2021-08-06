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

package org.apache.hadoop.ozone.om.request.database;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.hm.OmDatabaseArgs;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.database.OMDatabaseDeleteResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteDatabaseRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteDatabaseResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.DATABASE_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.USER_LOCK;

/**
 * Handles database delete request.
 */
public class OMDatabaseDeleteRequest extends OMDatabaseRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMDatabaseDeleteRequest.class);

  public OMDatabaseDeleteRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {

    DeleteDatabaseRequest deleteDatabaseRequest =
        getOmRequest().getDeleteDatabaseRequest();
    Preconditions.checkNotNull(deleteDatabaseRequest);

    String database = deleteDatabaseRequest.getName();

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumDatabaseDeletes();

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    boolean acquiredUserLock = false;
    boolean acquiredDatabaseLock = false;
    IOException exception = null;
    String owner = null;
    OMClientResponse omClientResponse = null;
    try {
      // check Acl
      if (ozoneManager.getAclsEnabled()) {
//        checkAcls(ozoneManager, OzoneObj.ResourceType.VOLUME,
//            OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.DELETE, volume,
//            null, null);
      }

      acquiredDatabaseLock = omMetadataManager.getLock().acquireWriteLock(
          DATABASE_LOCK, database);

      OmDatabaseArgs omDatabaseArgs = getDatabaseInfo(omMetadataManager, database);

      owner = omDatabaseArgs.getOwnerName();
      acquiredUserLock = omMetadataManager.getLock().acquireWriteLock(USER_LOCK,
          owner);

      String dbUserKey = omMetadataManager.getUserKey(owner);
      String dbDatabaseKey = omMetadataManager.getDatabaseKey(database);

      if (!omMetadataManager.isDatabaseEmpty(database)) {
        LOG.debug("database:{} is not empty", database);
        throw new OMException(OMException.ResultCodes.DATABASE_NOT_EMPTY);
      }

      OzoneManagerStorageProtos.PersistedUserDatabaseInfo newDatabaseList =
          omMetadataManager.getUserTableDb().get(owner);

      // delete the database from the owner list
      // as well as delete the database entry
      newDatabaseList = delDatabaseFromOwnerList(newDatabaseList, database, owner,
          transactionLogIndex);

      omMetadataManager.getUserTableDb().addCacheEntry(new CacheKey<>(dbUserKey),
          new CacheValue<>(Optional.of(newDatabaseList), transactionLogIndex));

      omMetadataManager.getDatabaseTable().addCacheEntry(
          new CacheKey<>(dbDatabaseKey), new CacheValue<>(Optional.absent(),
              transactionLogIndex));

      omResponse.setDeleteDatabaseResponse(
          DeleteDatabaseResponse.newBuilder().build());
      omClientResponse = new OMDatabaseDeleteResponse(omResponse.build(),
          database, owner, newDatabaseList);

    } catch (IOException ex) {
      exception = ex;
      omClientResponse = new OMDatabaseDeleteResponse(
          createErrorOMResponse(omResponse, exception));
    } finally {
      addResponseToDoubleBuffer(transactionLogIndex, omClientResponse,
          ozoneManagerDoubleBufferHelper);
      if (acquiredUserLock) {
        omMetadataManager.getLock().releaseWriteLock(USER_LOCK, owner);
      }
      if (acquiredDatabaseLock) {
        omMetadataManager.getLock().releaseWriteLock(DATABASE_LOCK, database);
      }
    }

    // Performing audit logging outside of the lock.
    auditLog(ozoneManager.getAuditLogger(),
        buildAuditMessage(OMAction.DELETE_DATABASE, buildVolumeAuditMap(database),
            exception, getOmRequest().getUserInfo()));

    // return response after releasing lock.
    if (exception == null) {
      LOG.debug("Database deleted for user:{} database:{}", owner, database);
      omMetrics.decNumDatabases();
    } else {
      LOG.error("Database deletion failed for user:{} database:{}",
          owner, database, exception);
      omMetrics.incNumDatabaseDeleteFails();
    }
    return omClientResponse;
  }
}

