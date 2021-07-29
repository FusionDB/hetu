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

package org.apache.hadoop.ozone.om.request.tablet;

import com.google.common.base.Optional;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OmPartitionInfo;
import org.apache.hadoop.ozone.om.helpers.OmTabletInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.tablet.OMTabletsDeleteResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteTabletsRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteTabletsResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.ozone.OzoneConsts.DATABASE;
import static org.apache.hadoop.ozone.OzoneConsts.DELETED_TABLETS_LIST;
import static org.apache.hadoop.ozone.OzoneConsts.PARTITION;
import static org.apache.hadoop.ozone.OzoneConsts.TABLE;
import static org.apache.hadoop.ozone.OzoneConsts.UNDELETED_TABLETS_LIST;
import static org.apache.hadoop.ozone.audit.OMAction.DELETE_TABLETS;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.PARTITION_LOCK;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.OK;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.PARTIAL_DELETE;

/**
 * Handles DeleteTablet request.
 */
public class OMTabletsDeleteRequest extends OMTabletRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMTabletsDeleteRequest.class);

  public OMTabletsDeleteRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  @SuppressWarnings("methodlength")
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long trxnLogIndex, OzoneManagerDoubleBufferHelper omDoubleBufferHelper) {
    DeleteTabletsRequest deleteTabletRequest =
        getOmRequest().getDeleteTabletsRequest();

    OzoneManagerProtocolProtos.DeleteTabletArgs deleteTabletArgs =
        deleteTabletRequest.getDeleteTablets();

    List<String> deleteTablets = new ArrayList<>(deleteTabletArgs.getTabletsList());

    IOException exception = null;
    OMClientResponse omClientResponse = null;
    Result result = null;

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumTabletDeletes();
    String databaseName = deleteTabletArgs.getDatabaseName();
    String tableName = deleteTabletArgs.getTableName();
    String partitionName = deleteTabletArgs.getPartitionName();
    Map<String, String> auditMap = new LinkedHashMap<>();
    auditMap.put(DATABASE, databaseName);
    auditMap.put(TABLE, tableName);
    auditMap.put(PARTITION, partitionName);
    List<OmTabletInfo> omTabletInfoList = new ArrayList<>();

    AuditLogger auditLogger = ozoneManager.getAuditLogger();
    OzoneManagerProtocolProtos.UserInfo userInfo =
        getOmRequest().getUserInfo();

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    boolean acquiredLock = false;

    int indexFailed = 0;
    int length = deleteTablets.size();
    OzoneManagerProtocolProtos.DeleteTabletArgs.Builder unDeletedTablets =
        OzoneManagerProtocolProtos.DeleteTabletArgs.newBuilder()
            .setDatabaseName(databaseName).setTableName(tableName)
            .setPartitionName(partitionName);

    boolean deleteStatus = true;
    try {
      acquiredLock = omMetadataManager.getLock().acquireWriteLock(PARTITION_LOCK,
          databaseName, tableName, partitionName);
      // Validate partition, table and database exists or not.
      validatePartitionAndTableAndDatabase(omMetadataManager, databaseName, tableName, partitionName);
      String databaseOwner = getDatabaseOwner(omMetadataManager, databaseName);

      for (indexFailed = 0; indexFailed < length; indexFailed++) {
        String tabletName = deleteTabletArgs.getTablets(indexFailed);
        String objectTablet = omMetadataManager.getOzoneTablet(databaseName, tableName,
            partitionName, tabletName);
        OmTabletInfo omTabletInfo = omMetadataManager.getTabletTable().get(objectTablet);

        if (omTabletInfo == null) {
          deleteStatus = false;
          LOG.error("Received a request to delete a Tablet does not exist {}",
              objectTablet);
          deleteTablets.remove(tabletName);
          unDeletedTablets.addTablets(tabletName);
          continue;
        }

        try {
          // TODO: check Acl
          omTabletInfoList.add(omTabletInfo);
        } catch (Exception ex) {
          deleteStatus = false;
          LOG.error("Acl check failed for tablet: {}", objectTablet, ex);
          deleteTablets.remove(tabletName);
          unDeletedTablets.addTablets(tabletName);
        }
      }

      long quotaReleased = 0;
      OmPartitionInfo omPartitionInfo =
          getPartitionInfo(omMetadataManager, databaseName, tableName, partitionName);

      // Mark all tablets which can be deleted, in cache as deleted.
      for (OmTabletInfo omTabletInfo : omTabletInfoList) {
        omMetadataManager.getTabletTable().addCacheEntry(
            new CacheKey<>(omMetadataManager.getOzoneTablet(databaseName, tableName, partitionName,
                omTabletInfo.getTabletName())),
            new CacheValue<>(Optional.absent(), trxnLogIndex));

        omTabletInfo.setUpdateID(trxnLogIndex, ozoneManager.isRatisEnabled());
        quotaReleased += sumBlockLengths(omTabletInfo);
      }
      omPartitionInfo.incrUsedBytes(-quotaReleased);
//      omPartitionInfo.incrUsedNamespace(-1L * omTabletInfoList.size());

      omClientResponse = new OMTabletsDeleteResponse(omResponse
          .setDeleteTabletsResponse(DeleteTabletsResponse.newBuilder()
              .setStatus(deleteStatus).setUnDeletedTablets(unDeletedTablets))
          .setStatus(deleteStatus ? OK : PARTIAL_DELETE)
          .setSuccess(deleteStatus).build(), omTabletInfoList,
          ozoneManager.isRatisEnabled(), omPartitionInfo.copyObject());

      result = Result.SUCCESS;

    } catch (IOException ex) {
      result = Result.FAILURE;
      exception = ex;
      createErrorOMResponse(omResponse, ex);

      // reset deleteTablets as request failed.
      deleteTablets = new ArrayList<>();
      // Add all tablets which are failed due to any other exception .
      for (int i = indexFailed; i < length; i++) {
        unDeletedTablets.addTablets(deleteTabletArgs.getTablets(i));
      }

      omResponse.setDeleteTabletsResponse(DeleteTabletsResponse.newBuilder()
          .setStatus(false).setUnDeletedTablets(unDeletedTablets).build()).build();
      omClientResponse = new OMTabletsDeleteResponse(omResponse.build());

    } finally {
      if (acquiredLock) {
        omMetadataManager.getLock().releaseWriteLock(PARTITION_LOCK, databaseName,
            tableName, partitionName);
      }
      addResponseToDoubleBuffer(trxnLogIndex, omClientResponse,
          omDoubleBufferHelper);
    }

    addDeletedTablets(auditMap, deleteTablets, unDeletedTablets.getTabletsList());

    auditLog(auditLogger, buildAuditMessage(DELETE_TABLETS, auditMap, exception,
        userInfo));


    switch (result) {
    case SUCCESS:
      omMetrics.decNumTablets(deleteTablets.size());
      if (LOG.isDebugEnabled()) {
        LOG.debug("Tablets delete success. Database:{}, Table:{}, Partition:{}, Tablets:{}",
            databaseName, tableName, partitionName, auditMap.get(DELETED_TABLETS_LIST));
      }
      break;
    case FAILURE:
      omMetrics.decNumTablets(deleteTablets.size());
      omMetrics.incNumTabletDeleteFails();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Tablets delete failed. Database:{}, Table:{}, Partition:{}, DeletedTablets:{}, " +
                "UnDeletedTablets:{}", databaseName, tableName, partitionName,
            auditMap.get(DELETED_TABLETS_LIST), auditMap.get(UNDELETED_TABLETS_LIST),
            exception);
      }
      break;
    default:
      LOG.error("Unrecognized Result for OMTabletsDeleteRequest: {}",
          deleteTabletRequest);
    }

    return omClientResponse;
  }

  /**
   * Add tablet info to audit map for DeleteTablets request.
   */
  private static void addDeletedTablets(
      Map<String, String> auditMap, List<String> deletedTablets,
      List<String> unDeletedTablets) {
    auditMap.put(DELETED_TABLETS_LIST, String.join(",", deletedTablets));
    auditMap.put(UNDELETED_TABLETS_LIST, String.join(",", unDeletedTablets));
  }

}
