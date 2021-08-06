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

package org.apache.hadoop.hetu.om.request.tablet;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.hetu.om.OMMetrics;
import org.apache.hadoop.hetu.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmPartitionInfo;
import org.apache.hadoop.ozone.om.helpers.OmTabletInfo;
import org.apache.hadoop.hetu.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.hetu.om.request.util.OmResponseUtil;
import org.apache.hadoop.hetu.om.response.OMClientResponse;
import org.apache.hadoop.hetu.om.response.tablet.OMTabletDeleteResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteTabletRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteTabletResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.TABLET_NOT_FOUND;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.PARTITION_LOCK;

/**
 * Handles DeleteTablet request.
 */
public class OMTabletDeleteRequest extends OMTabletRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMTabletDeleteRequest.class);

  public OMTabletDeleteRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    DeleteTabletRequest deleteTabletRequest = getOmRequest().getDeleteTabletRequest();
    Preconditions.checkNotNull(deleteTabletRequest);

    OzoneManagerProtocolProtos.TabletArgs tabletArgs = deleteTabletRequest.getTabletArgs();

    OzoneManagerProtocolProtos.TabletArgs.Builder newTabletArgs =
        tabletArgs.toBuilder().setModificationTime(Time.now())
            .setTabletName(validateAndNormalizeTablet(tabletArgs.getTabletName()));

    return getOmRequest().toBuilder()
        .setDeleteTabletRequest(deleteTabletRequest.toBuilder()
            .setTabletArgs(newTabletArgs))
        .setUserInfo(getUserIfNotExists(ozoneManager)).build();
  }

  @Override
  @SuppressWarnings("methodlength")
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long trxnLogIndex, OzoneManagerDoubleBufferHelper omDoubleBufferHelper) {
    DeleteTabletRequest deleteTabletRequest = getOmRequest().getDeleteTabletRequest();

    OzoneManagerProtocolProtos.TabletArgs tabletArgs =
        deleteTabletRequest.getTabletArgs();
    Map<String, String> auditMap = buildTabletArgsAuditMap(tabletArgs);

    String databaseName = tabletArgs.getDatabaseName();
    String tableName = tabletArgs.getTableName();
    String partitionName = tabletArgs.getPartitionName();
    String tabletName = tabletArgs.getTabletName();

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumTabletDeletes();

    AuditLogger auditLogger = ozoneManager.getAuditLogger();
    OzoneManagerProtocolProtos.UserInfo userInfo = getOmRequest().getUserInfo();

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    IOException exception = null;
    boolean acquiredLock = false;
    OMClientResponse omClientResponse = null;
    Result result = null;
    OmPartitionInfo omPartitionInfo = null;
    try {
      // TODO: check Acl

      String objectTablet = omMetadataManager.getOzoneTablet(
          databaseName, tableName, partitionName, tabletName);

      acquiredLock = omMetadataManager.getLock().acquireWriteLock(PARTITION_LOCK,
          databaseName, tableName, partitionName);

      // Validate partition and table and database exists or not.
      validatePartitionAndTableAndDatabase(omMetadataManager, databaseName, tableName, partitionName);

      OmTabletInfo omTabletInfo = omMetadataManager.getTabletTable().get(objectTablet);
      if (omTabletInfo == null) {
        throw new OMException("Tablet not found", TABLET_NOT_FOUND);
      }

      // Set the UpdateID to current transactionLogIndex
      omTabletInfo.setUpdateID(trxnLogIndex, ozoneManager.isRatisEnabled());

      // Update table cache.
      omMetadataManager.getTabletTable().addCacheEntry(
          new CacheKey<>(omMetadataManager.getOzoneTablet(databaseName, tableName,
              partitionName, tabletName)),
          new CacheValue<>(Optional.absent(), trxnLogIndex));

      omPartitionInfo = getPartitionInfo(omMetadataManager, databaseName, tableName, partitionName);

      long quotaReleased = sumBlockLengths(omTabletInfo);
      omPartitionInfo.incrUsedBytes(-quotaReleased);
//      omPartitionInfo.incrUsedNamespace(-1L);

      // No need to add cache entries to delete table. As delete table will
      // be used by DeleteTabletService only, not used for any client response
      // validation, so we don't need to add to cache.
      // TODO: Revisit if we need it later.

      omClientResponse = new OMTabletDeleteResponse(omResponse
          .setDeleteTabletResponse(DeleteTabletResponse.newBuilder()).build(),
          omTabletInfo, ozoneManager.isRatisEnabled(),
          omPartitionInfo.copyObject());

      result = Result.SUCCESS;
    } catch (IOException ex) {
      result = Result.FAILURE;
      exception = ex;
      omClientResponse = new OMTabletDeleteResponse(
          createErrorOMResponse(omResponse, exception));
    } finally {
      addResponseToDoubleBuffer(trxnLogIndex, omClientResponse,
            omDoubleBufferHelper);
      if (acquiredLock) {
        omMetadataManager.getLock().releaseWriteLock(PARTITION_LOCK, databaseName,
            tableName, partitionName);
      }
    }

    // Performing audit logging outside of the lock.
    auditLog(auditLogger, buildAuditMessage(OMAction.DELETE_TABLET, auditMap,
        exception, userInfo));


    switch (result) {
    case SUCCESS:
      omMetrics.decNumTablets();
      LOG.debug("Tablet deleted. Database:{}, Table:{}, Partition:{}, Tablet:{}", databaseName,
          tableName, partitionName, tabletName);
      break;
    case FAILURE:
      omMetrics.incNumTabletDeleteFails();
      LOG.error("Tablet delete failed. Database:{}, Table:{}, Partition:{}, Tablet:{}.",
          databaseName, tableName, partitionName, tabletName, exception);
      break;
    default:
      LOG.error("Unrecognized Result for OMTabletDeleteRequest: {}",
          deleteTabletRequest);
    }

    return omClientResponse;
  }
}
