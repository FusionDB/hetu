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
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.hm.OmDatabaseArgs;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.database.OMDatabaseSetOwnerResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetDatabasePropertyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetDatabasePropertyResponse;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.DATABASE_LOCK;

/**
 * Handle set owner request for database.
 */
public class OMDatabaseSetOwnerRequest extends OMDatabaseRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMDatabaseSetOwnerRequest.class);

  public OMDatabaseSetOwnerRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {

    long modificationTime = Time.now();
    OmDatabaseArgs databaseArgs = OmDatabaseArgs.getFromProtobuf(getOmRequest().getCreateDatabaseRequest().getDatabaseInfo());
    databaseArgs.setModificationTime(modificationTime);

    SetDatabasePropertyRequest.Builder setPropertyRequestBuilder = getOmRequest()
        .getSetDatabasePropertyRequest().toBuilder()
        .setDatabaseInfo(databaseArgs.getProtobuf());

    return getOmRequest().toBuilder()
        .setSetDatabasePropertyRequest(setPropertyRequestBuilder)
        .setUserInfo(getUserInfo())
        .build();
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {
    SetDatabasePropertyRequest setDatabasePropertyRequest =
        getOmRequest().getSetDatabasePropertyRequest();
    Preconditions.checkNotNull(setDatabasePropertyRequest);

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());
    // In production this will never happen, this request will be called only
    // when we have ownerName in setDatabasePropertyRequest.
    if (!setDatabasePropertyRequest.getDatabaseInfo().hasOwnerName()) {
      omResponse.setStatus(OzoneManagerProtocolProtos.Status.INVALID_REQUEST)
          .setSuccess(false);
      return new OMDatabaseSetOwnerResponse(omResponse.build());
    }

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumDatabaseUpdates();
    String database = setDatabasePropertyRequest.getDatabaseInfo().getName();
    String newOwner = setDatabasePropertyRequest.getDatabaseInfo().getOwnerName();

    AuditLogger auditLogger = ozoneManager.getAuditLogger();
    OzoneManagerProtocolProtos.UserInfo userInfo = getOmRequest().getUserInfo();

    Map<String, String> auditMap = buildDatabaseAuditMap(database);
    auditMap.put(OzoneConsts.OWNER, newOwner);

    boolean acquiredUserLocks = false;
    boolean acquiredDatabaseLock = false;
    IOException exception = null;
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    String oldOwner = null;
    OMClientResponse omClientResponse = null;
    try {
      // check Acl
      if (ozoneManager.getAclsEnabled()) {
//        checkAcls(ozoneManager, OzoneObj.ResourceType.DATABASE,
//            OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.WRITE_ACL,
//            database, null, null);
      }

      long maxUserDatabaseCount = ozoneManager.getMaxUserDatabaseCount();
      OzoneManagerStorageProtos.PersistedUserDatabaseInfo oldOwnerDatabaseList;
      OzoneManagerStorageProtos.PersistedUserDatabaseInfo newOwnerDatabaseList;
      OmDatabaseArgs omDatabaseArgs = null;

      acquiredDatabaseLock = omMetadataManager.getLock().acquireWriteLock(
          DATABASE_LOCK, database);
      omDatabaseArgs = getDatabaseInfo(omMetadataManager, database);
      oldOwner = omDatabaseArgs.getOwnerName();

      // Return OK immediately if newOwner is the same as oldOwner.
      if (oldOwner.equals(newOwner)) {
        LOG.warn("Database '{}' owner is already user '{}'.", database, oldOwner);
        omResponse.setStatus(OzoneManagerProtocolProtos.Status.OK)
          .setMessage(
            "Database '" + database + "' owner is already '" + newOwner + "'.")
            .setSuccess(false);
        omResponse.setSetDatabasePropertyResponse(
            SetDatabasePropertyResponse.newBuilder().setResponse(false).build());
        omClientResponse = new OMDatabaseSetOwnerResponse(omResponse.build());
        // Note: addResponseToDoubleBuffer would be executed in finally block.
        return omClientResponse;
      }

      acquiredUserLocks =
          omMetadataManager.getLock().acquireMultiUserLock(newOwner, oldOwner);
      oldOwnerDatabaseList =
          omMetadataManager.getUserTableDb().get(oldOwner);
      oldOwnerDatabaseList = delDatabaseFromOwnerList(
          oldOwnerDatabaseList, database, oldOwner, transactionLogIndex);
      newOwnerDatabaseList = omMetadataManager.getUserTableDb().get(newOwner);
      newOwnerDatabaseList = addDatabaseToOwnerList(
          newOwnerDatabaseList, database, newOwner,
          maxUserDatabaseCount, transactionLogIndex);

      // Set owner with new owner name.
      omDatabaseArgs.setOwnerName(newOwner);
      omDatabaseArgs.setUpdateID(transactionLogIndex,
          ozoneManager.isRatisEnabled());

      // Update modificationTime.
      omDatabaseArgs.setModificationTime(
          setDatabasePropertyRequest.getDatabaseInfo().getModificationTime());

      // Update cache.
      omMetadataManager.getUserTableDb().addCacheEntry(
          new CacheKey<>(omMetadataManager.getUserKey(newOwner)),
          new CacheValue<>(Optional.of(newOwnerDatabaseList),
              transactionLogIndex));
      omMetadataManager.getUserTableDb().addCacheEntry(
          new CacheKey<>(omMetadataManager.getUserKey(oldOwner)),
          new CacheValue<>(Optional.of(oldOwnerDatabaseList),
              transactionLogIndex));
      omMetadataManager.getDatabaseTable().addCacheEntry(
          new CacheKey<>(omMetadataManager.getDatabaseKey(database)),
          new CacheValue<>(Optional.of(omDatabaseArgs), transactionLogIndex));

      omResponse.setSetDatabasePropertyResponse(
          SetDatabasePropertyResponse.newBuilder().setResponse(true).build());
      omClientResponse = new OMDatabaseSetOwnerResponse(omResponse.build(),
          oldOwner, oldOwnerDatabaseList, newOwnerDatabaseList, omDatabaseArgs);
    } catch (IOException ex) {
      exception = ex;
      omClientResponse = new OMDatabaseSetOwnerResponse(
          createErrorOMResponse(omResponse, exception));
    } finally {
      addResponseToDoubleBuffer(transactionLogIndex, omClientResponse,
          ozoneManagerDoubleBufferHelper);
      if (acquiredUserLocks) {
        omMetadataManager.getLock().releaseMultiUserLock(newOwner, oldOwner);
      }
      if (acquiredDatabaseLock) {
        omMetadataManager.getLock().releaseWriteLock(DATABASE_LOCK, database);
      }
    }

    // Performing audit logging outside of the lock.
    auditLog(auditLogger, buildAuditMessage(OMAction.SET_OWNER, auditMap,
        exception, userInfo));

    // return response after releasing lock.
    if (exception == null) {
      LOG.debug("Successfully changed Owner of Database {} from {} -> {}", database,
          oldOwner, newOwner);
    } else {
      LOG.error("Changing database ownership failed for user:{} database:{}",
          newOwner, database, exception);
      omMetrics.incNumDatabaseUpdateFails();
    }
    return omClientResponse;
  }
}

