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
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.hetu.om.OMMetrics;
import org.apache.hadoop.hetu.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmPartitionInfo;
import org.apache.hadoop.ozone.om.helpers.OmTabletInfo;
import org.apache.hadoop.ozone.om.helpers.OmTabletLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.hetu.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.hetu.om.request.util.OmResponseUtil;
import org.apache.hadoop.hetu.om.response.OMClientResponse;
import org.apache.hadoop.hetu.om.response.tablet.OMTabletCommitResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CommitTabletRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TabletArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TabletLocation;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_A_FILE;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.TABLET_NOT_FOUND;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.PARTITION_LOCK;

/**
 * Handles CommitTablet request.
 */
public class OMTabletCommitRequest extends OMTabletRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMTabletCommitRequest.class);

  public OMTabletCommitRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    CommitTabletRequest commitTabletRequest = getOmRequest().getCommitTabletRequest();
    Preconditions.checkNotNull(commitTabletRequest);

    TabletArgs tabletArgs = commitTabletRequest.getTabletArgs();

    // Verify tablet name
    final boolean checkTabletNameEnabled = ozoneManager.getConfiguration()
         .getBoolean(OMConfigKeys.OZONE_OM_TABLET_NAME_CHARACTER_CHECK_ENABLED_KEY,
                 OMConfigKeys.OZONE_OM_TABLET_NAME_CHARACTER_CHECK_ENABLED_DEFAULT);
    if(checkTabletNameEnabled){
      OmUtils.validateTabletName(StringUtils.removeEnd(tabletArgs.getTabletName(),
              OzoneConsts.FS_FILE_COPYING_TEMP_SUFFIX));
    }

    // TODO: remove file system paths
    TabletArgs.Builder newTabletArgs =
        tabletArgs.toBuilder().setModificationTime(Time.now())
            .setTabletName(validateAndNormalizeTablet(
                ozoneManager.getEnableFileSystemPaths(), tabletArgs.getTabletName()));

    return getOmRequest().toBuilder()
        .setCommitTabletRequest(commitTabletRequest.toBuilder()
            .setTabletArgs(newTabletArgs)).setUserInfo(getUserInfo()).build();
  }

  @Override
  @SuppressWarnings("methodlength")
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long trxnLogIndex, OzoneManagerDoubleBufferHelper omDoubleBufferHelper) {

    CommitTabletRequest commitTabletRequest = getOmRequest().getCommitTabletRequest();

    TabletArgs commitTabletArgs = commitTabletRequest.getTabletArgs();

    String databaseName = commitTabletArgs.getDatabaseName();
    String tableName = commitTabletArgs.getTableName();
    String partitionName = commitTabletArgs.getPartitionName();
    String tabletName = commitTabletArgs.getTabletName();

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumTabletCommits();

    AuditLogger auditLogger = ozoneManager.getAuditLogger();

    Map<String, String> auditMap = buildTabletArgsAuditMap(commitTabletArgs);

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());

    IOException exception = null;
    OmTabletInfo omTabletInfo = null;
    OmPartitionInfo omPartitionInfo = null;
    OMClientResponse omClientResponse = null;
    boolean partitionLockAcquired = false;
    Result result;

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    try {
//      commitTabletArgs = resolvePartitionLink(ozoneManager, commitTabletArgs, auditMap);
      databaseName = commitTabletArgs.getDatabaseName();
      tableName = commitTabletArgs.getTableName();
      partitionName = commitTabletArgs.getPartitionName();

      // TODO check Acl

      String dbOzoneTablet =
          omMetadataManager.getOzoneTablet(databaseName, tableName,
              partitionName, tabletName);
      String dbOpenTablet = omMetadataManager.getOpenTablet(databaseName, tableName,
          partitionName, tabletName, commitTabletRequest.getClientID());

      List<OmTabletLocationInfo> locationInfoList = new ArrayList<>();
      for (TabletLocation tabletLocation : commitTabletArgs.getTabletLocationsList()) {
        locationInfoList.add(OmTabletLocationInfo.getFromProtobuf(tabletLocation));
      }

      partitionLockAcquired =
          omMetadataManager.getLock().acquireWriteLock(PARTITION_LOCK,
              databaseName, tableName, partitionName);

      validatePartitionAndTableAndDatabase(omMetadataManager, databaseName, tableName, partitionName);

      // TODO: Remove - Check for directory exists with same name, if it exists throw error.
      if (ozoneManager.getEnableFileSystemPaths()) {
        if (checkDirectoryAlreadyExists(databaseName, tableName, partitionName, tabletName,
            omMetadataManager)) {
          throw new OMException("Can not create tablet: " + tabletName +
              " as there is already directory in the given path", NOT_A_FILE);
        }
        // Ensure the parent exist.
        if (!"".equals(OzoneFSUtils.getParent(tabletName))
            && !checkDirectoryAlreadyExists(databaseName, tableName, partitionName,
            OzoneFSUtils.getParent(tabletName), omMetadataManager)) {
          throw new OMException("Cannot create tablet : " + tabletName
              + " as parent directory doesn't exist",
              OMException.ResultCodes.DIRECTORY_NOT_FOUND);
        }
      }

      omTabletInfo = omMetadataManager.getOpenTabletTable().get(dbOpenTablet);
      if (omTabletInfo == null) {
        throw new OMException("Failed to commit tablet, as " + dbOpenTablet +
            "entry is not found in the OpenTablet table", TABLET_NOT_FOUND);
      }
      omTabletInfo.setDataSize(commitTabletArgs.getDataSize());

      omTabletInfo.setModificationTime(commitTabletArgs.getModificationTime());

      // Update the block length for each block
      List<OmTabletLocationInfo> allocatedLocationInfoList =
          omTabletInfo.getLatestVersionLocations().getLocationList();
      omTabletInfo.updateLocationInfoList(locationInfoList, false);

      // Set the UpdateID to current transactionLogIndex
      omTabletInfo.setUpdateID(trxnLogIndex, ozoneManager.isRatisEnabled());

      // Add to cache of open tablet table and tablet table.
      omMetadataManager.getOpenTabletTable().addCacheEntry(
          new CacheKey<>(dbOpenTablet),
          new CacheValue<>(Optional.absent(), trxnLogIndex));

      omMetadataManager.getTabletTable().addCacheEntry(
          new CacheKey<>(dbOzoneTablet),
          new CacheValue<>(Optional.of(omTabletInfo), trxnLogIndex));

      long scmBlockSize = ozoneManager.getScmBlockSize();
      int factor = omTabletInfo.getFactor().getNumber();
      omPartitionInfo = getPartitionInfo(omMetadataManager, databaseName, tableName, partitionName);
      // Block was pre-requested and UsedBytes updated when createKey and
      // AllocatedBlock. The space occupied by the Key shall be based on
      // the actual Key size, and the total Block size applied before should
      // be subtracted.
      long correctedSpace = omTabletInfo.getDataSize() * factor -
          allocatedLocationInfoList.size() * scmBlockSize * factor;
      omPartitionInfo.incrUsedBytes(correctedSpace);

      omClientResponse = new OMTabletCommitResponse(omResponse.build(),
          omTabletInfo, dbOzoneTablet, dbOpenTablet, omPartitionInfo.copyObject());

      result = Result.SUCCESS;
    } catch (IOException ex) {
      result = Result.FAILURE;
      exception = ex;
      omClientResponse = new OMTabletCommitResponse(createErrorOMResponse(
          omResponse, exception));
    } finally {
      addResponseToDoubleBuffer(trxnLogIndex, omClientResponse,
          omDoubleBufferHelper);

      if(partitionLockAcquired) {
        omMetadataManager.getLock().releaseWriteLock(PARTITION_LOCK, databaseName,
            tableName, partitionName);
      }
    }

    auditLog(auditLogger, buildAuditMessage(OMAction.COMMIT_TABLET, auditMap,
          exception, getOmRequest().getUserInfo()));

    switch (result) {
    case SUCCESS:
      // As when we commit the key, then it is visible in ozone, so we should
      // increment here.
      // As key also can have multiple versions, we need to increment keys
      // only if version is 0. Currently we have not complete support of
      // versioning of keys. So, this can be revisited later.
      if (omTabletInfo.getTabletLocationVersions().size() == 1) {
        omMetrics.incNumTablets();
      }
      LOG.debug("Tablet committed. Database:{}, Table:{}, Partition:{}, Tablet: {}", databaseName,
          tableName, partitionName, tabletName);
      break;
    case FAILURE:
      LOG.error("Tablet commit failed. Database:{}, Table:{}, Partition:{}, Tablet: {}.",
          databaseName, tableName, partitionName, tabletName, exception);
      omMetrics.incNumTabletCommitFails();
      break;
    default:
      LOG.error("Unrecognized Result for OMTabletCommitRequest: {}",
          commitTabletRequest);
    }

    return omClientResponse;
  }
}
