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
import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.hm.OmDatabaseArgs;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmPartitionInfo;
import org.apache.hadoop.ozone.om.helpers.OmTableInfo;
import org.apache.hadoop.ozone.om.helpers.OmTabletInfo;
import org.apache.hadoop.ozone.om.helpers.OmTabletLocationInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.tablet.OMAllocateTabletResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AllocateTabletRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AllocateTabletResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TabletArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.TABLET_NOT_FOUND;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.PARTITION_LOCK;

/**
 * Handles allocate tablet block request.
 */
public class OMAllocateTabletRequest extends OMTabletRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMAllocateTabletRequest.class);

  public OMAllocateTabletRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {

    AllocateTabletRequest allocateTabletRequest =
        getOmRequest().getAllocateTabletRequest();

    Preconditions.checkNotNull(allocateTabletRequest);

    TabletArgs tabletArgs = allocateTabletRequest.getTabletArgs();

    ExcludeList excludeList = new ExcludeList();
    if (allocateTabletRequest.hasExcludeList()) {
      excludeList =
          ExcludeList.getFromProtoBuf(allocateTabletRequest.getExcludeList());
    }

    // TODO: Here we are allocating block with out any check for tablet exist in
    //  open table or not and also with out any authorization checks.
    //  Assumption here is that allocateTabletBlocks with out openTablet will be less.
    //  There is a chance some one can misuse this api to flood allocateBlock
    //  calls. But currently allocateBlock is internally called from
    //  BlockOutputStreamEntryPool, so we are fine for now. But if one some
    //  one uses direct omclient we might be in trouble.


    // To allocate atleast one tablet block passing requested size and scmBlockSize
    // as same value. When allocating tablet block requested size is same as
    // scmBlockSize.
    List<OmTabletLocationInfo> omTabletLocationInfoList =
        allocateBlock(ozoneManager.getScmClient(),
            ozoneManager.getBlockTokenSecretManager(), tabletArgs.getType(),
            tabletArgs.getFactor(), excludeList, ozoneManager.getScmBlockSize(),
            ozoneManager.getScmBlockSize(),
            ozoneManager.getPreallocateBlocksMax(),
            ozoneManager.isGrpcBlockTokenEnabled(), ozoneManager.getOMNodeId());

    // Set modification time and normalize tablet if required.
    TabletArgs.Builder newTabletArgs = tabletArgs.toBuilder()
        .setModificationTime(Time.now())
        // TODO: remove EnableFileSystemPaths
        .setTabletName(validateAndNormalizeTablet(
            ozoneManager.getEnableFileSystemPaths(), tabletArgs.getTabletName()));

    AllocateTabletRequest.Builder newAllocatedTabletRequest =
        AllocateTabletRequest.newBuilder()
            .setClientID(allocateTabletRequest.getClientID())
            .setTabletArgs(newTabletArgs);

    if (allocateTabletRequest.hasExcludeList()) {
      newAllocatedTabletRequest.setExcludeList(
          allocateTabletRequest.getExcludeList());
    }

    // Add allocated tablet block info.
    newAllocatedTabletRequest.setTabletLocation(
        omTabletLocationInfoList.get(0).getProtobuf(getOmRequest().getVersion()));

    return getOmRequest().toBuilder().setUserInfo(getUserInfo())
        .setAllocateTabletRequest(newAllocatedTabletRequest).build();

  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long trxnLogIndex, OzoneManagerDoubleBufferHelper omDoubleBufferHelper) {

    AllocateTabletRequest allocateTabletRequest =
        getOmRequest().getAllocateTabletRequest();

    TabletArgs tabletArgs =
        allocateTabletRequest.getTabletArgs();

    OzoneManagerProtocolProtos.TabletLocation tabletLocation =
        allocateTabletRequest.getTabletLocation();
    Preconditions.checkNotNull(tabletLocation);

    String databaseName = tabletArgs.getDatabaseName();
    String tableName = tabletArgs.getTableName();
    String partitionName = tabletArgs.getPartitionName();
    String tabletName = tabletArgs.getTabletName();
    long clientID = allocateTabletRequest.getClientID();

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumTabletAllocateCalls();

    AuditLogger auditLogger = ozoneManager.getAuditLogger();

    Map<String, String> auditMap = buildTabletArgsAuditMap(tabletArgs);
    auditMap.put(OzoneConsts.CLIENT_ID, String.valueOf(clientID));

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    String openTabletName = null;

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());
    OMClientResponse omClientResponse = null;

    OmTabletInfo openTabletInfo = null;
    IOException exception = null;
    OmPartitionInfo omPartitionInfo = null;
    boolean acquiredLock = false;

    try {
      // TODO: check Acl
      validatePartitionAndTableAndDatabase(omMetadataManager, databaseName, tableName,
          partitionName);

      // Here we don't acquire partition/table/database lock because for a single client
      // allocateTablet block is called in serial fashion.

      openTabletName = omMetadataManager.getOpenTablet(databaseName, tableName, partitionName,
          tabletName, clientID);
      openTabletInfo = omMetadataManager.getOpenTabletTable().get(openTabletName);
      if (openTabletInfo == null) {
        throw new OMException("Open Tablet not found " + openTabletName,
            TABLET_NOT_FOUND);
      }

      List<OmTabletLocationInfo> newLocationList = Collections.singletonList(
          OmTabletLocationInfo.getFromProtobuf(tabletLocation));

      acquiredLock = omMetadataManager.getLock().acquireWriteLock(PARTITION_LOCK,
          databaseName, tableName, partitionName);
      OmDatabaseArgs omDatabaseArgs = getDatabaseInfo(omMetadataManager, databaseName);
      OmTableInfo omTableInfo = getTableInfo(omMetadataManager, databaseName, tableName);
      omPartitionInfo = getPartitionInfo(omMetadataManager, databaseName, tableName, partitionName);
      // check database quota
      long preAllocatedSpace = newLocationList.size()
          * ozoneManager.getScmBlockSize()
          * openTabletInfo.getFactor().getNumber();
      checkTableQuotaInDatabase(omDatabaseArgs, omTableInfo, preAllocatedSpace);
      // Append new tablet block
      openTabletInfo.appendNewBlocks(newLocationList, false);

      // Set modification time.
      openTabletInfo.setModificationTime(tabletArgs.getModificationTime());

      // Set the UpdateID to current transactionLogIndex
      openTabletInfo.setUpdateID(trxnLogIndex, ozoneManager.isRatisEnabled());

      // Add to cache.
      omMetadataManager.getOpenTabletTable().addCacheEntry(
          new CacheKey<>(openTabletName),
          new CacheValue<>(Optional.of(openTabletInfo), trxnLogIndex));

      omPartitionInfo.incrUsedBytes(preAllocatedSpace);
      omResponse.setAllocateTabletResponse(AllocateTabletResponse.newBuilder()
          .setTabletLocation(tabletLocation).build());
      omClientResponse = new OMAllocateTabletResponse(omResponse.build(),
          openTabletInfo, clientID, omPartitionInfo.copyObject());

      LOG.debug("Allocated tablet block for Database:{}, Table:{}, Partition: {}, OpenTablet:{}",
          databaseName, tableName, partitionName, openTabletName);
    } catch (IOException ex) {
      omMetrics.incNumTabletAllocateCallFails();
      exception = ex;
      omClientResponse = new OMAllocateTabletResponse(createErrorOMResponse(
          omResponse, exception));
      LOG.error("Allocate Tablet Block failed. Database:{}, Tablet:{}, Partition: {}, OpenTablet:{}. " +
            "Exception:{}", databaseName, tableName, partitionName, tabletName, exception);
    } finally {
      addResponseToDoubleBuffer(trxnLogIndex, omClientResponse,
          omDoubleBufferHelper);
      if (acquiredLock) {
        omMetadataManager.getLock().releaseWriteLock(PARTITION_LOCK, databaseName, tableName,
            partitionName);
      }
    }

    auditLog(auditLogger, buildAuditMessage(OMAction.ALLOCATE_TABLET, auditMap,
        exception, getOmRequest().getUserInfo()));

    return omClientResponse;
  }
}
