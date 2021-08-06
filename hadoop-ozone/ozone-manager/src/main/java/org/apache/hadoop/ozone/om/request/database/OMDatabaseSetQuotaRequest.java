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
import org.apache.hadoop.ozone.om.helpers.OmTableInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.database.OMDatabaseSetQuotaResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetDatabasePropertyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetDatabasePropertyResponse;

import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.DATABASE_LOCK;

/**
 * Handles set Quota request for database.
 */
public class OMDatabaseSetQuotaRequest extends OMDatabaseRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMDatabaseSetQuotaRequest.class);

  public OMDatabaseSetQuotaRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {

    long modificationTime = Time.now();
    OmDatabaseArgs databaseArgs = OmDatabaseArgs.getFromProtobuf(getOmRequest().getSetDatabasePropertyRequest().getDatabaseInfo());
    databaseArgs.setModificationTime(modificationTime);

    SetDatabasePropertyRequest.Builder setPropertyRequestBuilde = getOmRequest()
        .getSetDatabasePropertyRequest().toBuilder()
        .setDatabaseInfo(databaseArgs.getProtobuf());

    return getOmRequest().toBuilder()
        .setSetDatabasePropertyRequest(setPropertyRequestBuilde)
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
    // when we have quota in bytes is set in setDatabasePropertyRequest.
    if (!setDatabasePropertyRequest.getDatabaseInfo().hasQuotaInBytes()) {
      omResponse.setStatus(OzoneManagerProtocolProtos.Status.INVALID_REQUEST)
          .setSuccess(false);
      return new OMDatabaseSetQuotaResponse(omResponse.build());
    }

    String database = setDatabasePropertyRequest.getDatabaseInfo().getName();
    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumDatabaseUpdates();

    AuditLogger auditLogger = ozoneManager.getAuditLogger();
    OzoneManagerProtocolProtos.UserInfo userInfo = getOmRequest().getUserInfo();
    Map<String, String> auditMap = buildDatabaseAuditMap(database);
    auditMap.put(OzoneConsts.QUOTA_IN_BYTES,
        String.valueOf(setDatabasePropertyRequest.getDatabaseInfo().getQuotaInBytes()));

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    IOException exception = null;
    boolean acquireDatabaseLock = false;
    OMClientResponse omClientResponse = null;
    try {
      // check Acl
      if (ozoneManager.getAclsEnabled()) {
//        checkAcls(ozoneManager, OzoneObj.ResourceType.DATABASE,
//            OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.WRITE, database,
//            null, null);
      }

      acquireDatabaseLock = omMetadataManager.getLock().acquireWriteLock(
          DATABASE_LOCK, database);

      OmDatabaseArgs omDatabaseArgs = getDatabaseInfo(omMetadataManager, database);
      if (checkQuotaBytesValid(omMetadataManager,
          setDatabasePropertyRequest.getDatabaseInfo().getQuotaInBytes(), database)) {
        omDatabaseArgs.setQuotaInBytes(
            setDatabasePropertyRequest
                    .getDatabaseInfo()
                    .getQuotaInBytes());
      } else {
        omDatabaseArgs.setQuotaInBytes(omDatabaseArgs.getQuotaInBytes());
      }
      if (checkQuotaNamespaceValid(
          setDatabasePropertyRequest.getDatabaseInfo().getQuotaInNamespace())) {
        omDatabaseArgs.setQuotaInNamespace(
            setDatabasePropertyRequest
                .getDatabaseInfo()
                .getQuotaInNamespace());
      } else {
        omDatabaseArgs.setQuotaInNamespace(omDatabaseArgs.getQuotaInNamespace());
      }

      omDatabaseArgs.setUpdateID(transactionLogIndex,
          ozoneManager.isRatisEnabled());
      omDatabaseArgs.setModificationTime(
          setDatabasePropertyRequest
                  .getDatabaseInfo()
                  .getModificationTime());

      // update cache.
      omMetadataManager.getDatabaseTable().addCacheEntry(
          new CacheKey<>(omMetadataManager.getDatabaseKey(database)),
          new CacheValue<>(Optional.of(omDatabaseArgs), transactionLogIndex));

      omResponse.setSetDatabasePropertyResponse(
          SetDatabasePropertyResponse.newBuilder().build());
      omClientResponse = new OMDatabaseSetQuotaResponse(omResponse.build(),
              omDatabaseArgs);
    } catch (IOException ex) {
      exception = ex;
      omClientResponse = new OMDatabaseSetQuotaResponse(
          createErrorOMResponse(omResponse, exception));
    } finally {
      addResponseToDoubleBuffer(transactionLogIndex, omClientResponse,
          ozoneManagerDoubleBufferHelper);
      if (acquireDatabaseLock) {
        omMetadataManager.getLock().releaseWriteLock(DATABASE_LOCK, database);
      }
    }

    // Performing audit logging outside of the lock.
    auditLog(auditLogger, buildAuditMessage(OMAction.SET_QUOTA, auditMap,
        exception, userInfo));

    // return response after releasing lock.
    if (exception == null) {
      LOG.debug("Changing database quota is successfully completed for database: " +
          "{} quota:{}", database, setDatabasePropertyRequest.getDatabaseInfo().getQuotaInBytes());
    } else {
      omMetrics.incNumDatabaseUpdateFails();
      LOG.error("Changing database quota failed for database:{} quota:{}", database,
          setDatabasePropertyRequest.getDatabaseInfo().getQuotaInBytes(), exception);
    }
    return omClientResponse;
  }

  public boolean checkQuotaBytesValid(OMMetadataManager metadataManager,
      long databaseQuotaInBytes, String databaseName) throws IOException {
    long totalTableQuota = 0;

    if (databaseQuotaInBytes < OzoneConsts.QUOTA_RESET
        || databaseQuotaInBytes == 0) {
      return false;
    }

    List<OmTableInfo> tableList = metadataManager.listMetaTables(
        databaseName, null, null, Integer.MAX_VALUE);
    for(OmTableInfo tableInfo : tableList) {
      long nextQuotaInBytes = tableInfo.getUsedCapacityInBytes();
      if(nextQuotaInBytes > OzoneConsts.QUOTA_RESET) {
        totalTableQuota += nextQuotaInBytes;
      }
    }
    if(databaseQuotaInBytes < totalTableQuota &&
        databaseQuotaInBytes != OzoneConsts.QUOTA_RESET) {
      throw new IllegalArgumentException("Total tables quota in this database " +
          "should not be greater than database quota : the total space quota is" +
          ":" + totalTableQuota + ". But the database space quota is:" +
          databaseQuotaInBytes);
    }
    return true;
  }

  public boolean checkQuotaNamespaceValid(long quotaInNamespace) {

    if (quotaInNamespace < OzoneConsts.QUOTA_RESET || quotaInNamespace == 0) {
      return false;
    }
    return true;
  }
}


