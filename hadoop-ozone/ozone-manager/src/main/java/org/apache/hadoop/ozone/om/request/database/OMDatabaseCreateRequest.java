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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.hetu.hm.helpers.OmDatabaseArgs;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.database.OMDatabaseCreateResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateDatabaseRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateDatabaseResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DatabaseInfo;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos.PersistedUserDatabaseInfo;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.DATABASE_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.USER_LOCK;

/**
 * Handles database create request.
 */
public class OMDatabaseCreateRequest extends OMDatabaseRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMDatabaseCreateRequest.class);

  public OMDatabaseCreateRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {

    DatabaseInfo databaseInfo  =
        getOmRequest().getCreateDatabaseRequest().getDatabaseInfo();
    // Verify resource name
    OmUtils.validateDatabaseName(databaseInfo.getName());

    // Set creation time & set modification time
    long initialTime = Time.now();
    DatabaseInfo updatedDatabaseInfo =
            databaseInfo.toBuilder()
            .setCreationTime(initialTime)
            .setModificationTime(initialTime)
            .build();

    return getOmRequest().toBuilder().setCreateDatabaseRequest(
        CreateDatabaseRequest.newBuilder().setDatabaseInfo(updatedDatabaseInfo))
        .setUserInfo(getUserInfo())
        .build();
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {

    CreateDatabaseRequest createDatabaseRequest =
        getOmRequest().getCreateDatabaseRequest();
    Preconditions.checkNotNull(createDatabaseRequest);
    DatabaseInfo databaseInfo = createDatabaseRequest.getDatabaseInfo();

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumDatabaseCreates();

    String database = databaseInfo.getName();
    String owner = databaseInfo.getOwnerName();

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    // Doing this here, so we can do protobuf conversion outside of lock.
    boolean acquiredDatabaseLock = false;
    boolean acquiredUserLock = false;
    IOException exception = null;
    OMClientResponse omClientResponse = null;
    OmDatabaseArgs omDatabaseArgs = null;
    Map<String, String> auditMap = new HashMap<>();
    try {
      omDatabaseArgs = OmDatabaseArgs.getFromProtobuf(databaseInfo);
      // when you create a database, we set both Object ID and update ID.
      // The Object ID will never change, but update
      // ID will be set to transactionID each time we update the object.
      omDatabaseArgs.setObjectID(
          ozoneManager.getObjectIdFromTxId(transactionLogIndex));
      omDatabaseArgs.setUpdateID(transactionLogIndex,
          ozoneManager.isRatisEnabled());


      auditMap = omDatabaseArgs.toAuditMap();

      // check acl
      if (ozoneManager.getAclsEnabled()) {
//        checkAcls(ozoneManager, OzoneObj.ResourceType.DATABASE,
//            OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.CREATE, database,
//            null, null);
      }

      // acquire lock.
      acquiredDatabaseLock = omMetadataManager.getLock().acquireWriteLock(
          DATABASE_LOCK, database);

      acquiredUserLock = omMetadataManager.getLock().acquireWriteLock(USER_LOCK,
          owner);

      String dbDatabaseKey = omMetadataManager.getDatabaseKey(database);

      PersistedUserDatabaseInfo databaseList = null;
      if (omMetadataManager.getDatabaseTable().isExist(dbDatabaseKey)) {
        LOG.debug("Database:{} already exists", omDatabaseArgs.getName());
        throw new OMException("Database already exists",
            OMException.ResultCodes.DATABASE_ALREADY_EXISTS);
      } else {
        String dbUserKey = omMetadataManager.getUserKey(owner);
        databaseList = omMetadataManager.getUserTableDb().get(dbUserKey);
        databaseList = addDatabaseToOwnerList(databaseList, database, owner,
            ozoneManager.getMaxUserDatabaseCount(), transactionLogIndex);
        createDatabase(omMetadataManager, omDatabaseArgs, databaseList, dbDatabaseKey,
            dbUserKey, transactionLogIndex);

        omResponse.setCreateDatabaseResponse(CreateDatabaseResponse.newBuilder()
            .build());
        omClientResponse = new OMDatabaseCreateResponse(omResponse.build(),
                omDatabaseArgs, databaseList);
        LOG.debug("database:{} successfully created", omDatabaseArgs.getName());
      }

    } catch (IOException ex) {
      exception = ex;
      omClientResponse = new OMDatabaseCreateResponse(
          createErrorOMResponse(omResponse, exception));
    } finally {
      if (omClientResponse != null) {
        omClientResponse.setFlushFuture(
            ozoneManagerDoubleBufferHelper.add(omClientResponse,
                transactionLogIndex));
      }
      if (acquiredUserLock) {
        omMetadataManager.getLock().releaseWriteLock(USER_LOCK, owner);
      }
      if (acquiredDatabaseLock) {
        omMetadataManager.getLock().releaseWriteLock(DATABASE_LOCK, database);
      }
    }

    // Performing audit logging outside of the lock.
    auditLog(ozoneManager.getAuditLogger(),
        buildAuditMessage(OMAction.CREATE_DATABASE, auditMap, exception,
            getOmRequest().getUserInfo()));

    // return response after releasing lock.
    if (exception == null) {
      LOG.info("created database:{} for user:{}", database, owner);
      omMetrics.incNumDatabases();
    } else {
      LOG.error("Database creation failed for user:{} database:{}", owner,
              database, exception);
      omMetrics.incNumDatabaseCreateFails();
    }
    return omClientResponse;
  }

  @VisibleForTesting
  public static Logger getLogger() {
    return LOG;
  }
}


