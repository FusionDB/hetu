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
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.utils.UniqueId;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.hm.OmDatabaseArgs;
import org.apache.hadoop.ozone.om.OMConfigKeys;
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
import org.apache.hadoop.ozone.om.response.tablet.OMTabletCreateResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateTabletRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateTabletResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TabletArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.PARTITION_LOCK;

/**
 * Handles CreateTablet request.
 */

public class OMTabletCreateRequest extends OMTabletRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMTabletCreateRequest.class);

  public OMTabletCreateRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    CreateTabletRequest createTabletRequest = getOmRequest().getCreateTabletRequest();
    Preconditions.checkNotNull(createTabletRequest);

    TabletArgs tabletArgs = createTabletRequest.getTabletArgs();

    // Verify tablet name
    final boolean checkTabletNameEnabled = ozoneManager.getConfiguration()
         .getBoolean(OMConfigKeys.OZONE_OM_TABLET_NAME_CHARACTER_CHECK_ENABLED_KEY,
                 OMConfigKeys.OZONE_OM_TABLET_NAME_CHARACTER_CHECK_ENABLED_DEFAULT);
    if(checkTabletNameEnabled){
      OmUtils.validateTabletName(tabletArgs.getTabletName());
    }

    // TODO: the ahead for remove
    String tabletPath = tabletArgs.getTabletName();
    if (ozoneManager.getEnableFileSystemPaths()) {
      // If enabled, disallow keys with trailing /. As in fs semantics
      // directories end with trailing /.
      tabletPath = validateAndNormalizeTablet(
          ozoneManager.getEnableFileSystemPaths(), tabletPath);
      if (tabletPath.endsWith("/")) {
        throw new OMException("Invalid tabletPath, tablet names with trailing / " +
            "are not allowed." + tabletPath,
            OMException.ResultCodes.INVALID_TABLET_NAME);
      }
    }

    // We cannot allocate block for multipart upload part when
    // createMultipartSegment is called, as we will not know type and factor with
    // which initiateMultipartUpload has started for this key. When
    // allocateBlock call happen's we shall know type and factor, as we set
    // the type and factor read from multipart table, and set the TabletInfo in
    // validateAndUpdateCache and return to the client. TODO: See if we can fix
    //  this. We do not call allocateBlock in openKey for multipart upload.

    CreateTabletRequest.Builder newCreateTabletRequest = null;
    TabletArgs.Builder newTabletArgs = null;
    // TODO: remove multipart
    if (!tabletArgs.getIsMultipartTablet()) {

      long scmBlockSize = ozoneManager.getScmBlockSize();

      // NOTE size of a key is not a hard limit on anything, it is a value that
      // client should expect, in terms of current size of key. If client sets
      // a value, then this value is used, otherwise, we allocate a single
      // block which is the current size, if read by the client.
      final long requestedSize = tabletArgs.getDataSize() > 0 ?
              tabletArgs.getDataSize() : scmBlockSize;

      boolean useRatis = ozoneManager.shouldUseRatis();

      HddsProtos.ReplicationFactor factor = tabletArgs.getFactor();
      if (factor == null) {
        factor = useRatis ? HddsProtos.ReplicationFactor.THREE :
            HddsProtos.ReplicationFactor.ONE;
      }

      HddsProtos.ReplicationType type = tabletArgs.getType();
      if (type == null) {
        type = useRatis ? HddsProtos.ReplicationType.RATIS :
            HddsProtos.ReplicationType.STAND_ALONE;
      }

      // TODO: Here we are allocating block with out any check for
      //  database/table/partition/tablet/block/chunks or not and also with out any authorization checks.
      //  As for a client for the first time this can be executed on any OM,
      //  till leader is identified.

      List<OmTabletLocationInfo> omTabletLocationInfoList =
          allocateBlock(ozoneManager.getScmClient(),
              ozoneManager.getBlockTokenSecretManager(), type, factor,
              new ExcludeList(), requestedSize, scmBlockSize,
              ozoneManager.getPreallocateBlocksMax(),
              ozoneManager.isGrpcBlockTokenEnabled(),
              ozoneManager.getOMNodeId());

      newTabletArgs = tabletArgs.toBuilder().setModificationTime(Time.now())
              .setType(type).setFactor(factor)
              .setDataSize(requestedSize);

      newTabletArgs.addAllTabletLocations(omTabletLocationInfoList.stream()
          .map(info -> info.getProtobuf(false,
              getOmRequest().getVersion()))
          .collect(Collectors.toList()));
    } else {
      newTabletArgs = tabletArgs.toBuilder().setModificationTime(Time.now());
    }

    newTabletArgs.setTabletName(tabletPath);

    newCreateTabletRequest =
        createTabletRequest.toBuilder().setTabletArgs(newTabletArgs)
            .setClientID(UniqueId.next());

    return getOmRequest().toBuilder()
        .setCreateTabletRequest(newCreateTabletRequest).setUserInfo(getUserInfo())
        .build();
  }

  @Override
  @SuppressWarnings("methodlength")
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long trxnLogIndex, OzoneManagerDoubleBufferHelper omDoubleBufferHelper) {
    CreateTabletRequest createTabletRequest = getOmRequest().getCreateTabletRequest();

    TabletArgs tabletArgs = createTabletRequest.getTabletArgs();
    Map<String, String> auditMap = buildTabletArgsAuditMap(tabletArgs);

    String databaseName = tabletArgs.getDatabaseName();
    String tableName = tabletArgs.getTableName();
    String partitionName = tabletArgs.getPartitionName();
    String tabletName = tabletArgs.getTabletName();

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumTabletAllocates();

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    OmTabletInfo omTabletInfo = null;
    OmPartitionInfo omPartitionInfo = null;
    final List< OmTabletLocationInfo > locations = new ArrayList<>();

    boolean acquireLock = false;
    OMClientResponse omClientResponse = null;
    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());
    IOException exception = null;
    Result result = null;
    List<OmTabletInfo> missingParentInfos = null;
    int numMissingParents = 0;
    try {
      // TODO: check Acl

      acquireLock = omMetadataManager.getLock().acquireWriteLock(PARTITION_LOCK,
          databaseName, tableName, partitionName);
      validatePartitionAndTableAndDatabase(omMetadataManager, databaseName, tableName, partitionName);
      //TODO: We can optimize this get here, if getKmsProvider is null, then
      // tablet encryptionInfo will be not set. If this assumption holds
      // true, we can avoid get from tablet meta table.

      // Check if tablet key already exists
      String dbTabletName = omMetadataManager.getOzoneTablet(databaseName, tableName,
          partitionName, tabletName);
      OmTabletInfo dbTabletInfo =
          omMetadataManager.getTabletTable().getIfExist(dbTabletName);

      if (dbTabletInfo != null) {
        ozoneManager.getKeyManager().refresh(dbTabletInfo);
      }

      OmPartitionInfo partitionInfo = omMetadataManager.getPartitionTable().get(
              omMetadataManager.getPartitionKey(databaseName, tableName, partitionName));

      omTabletInfo = prepareTabletInfo(omMetadataManager, tabletArgs, dbTabletInfo,
          tabletArgs.getDataSize(), locations, partitionInfo, trxnLogIndex,
          ozoneManager.getObjectIdFromTxId(trxnLogIndex),
          ozoneManager.isRatisEnabled());

      long openVersion = omTabletInfo.getLatestVersionLocations().getVersion();
      long clientID = createTabletRequest.getClientID();
      String dbOpenTabletName = omMetadataManager.getOpenTablet(databaseName,
          tableName, partitionName, tabletName, clientID);

      // Append new tablets
      List<OmTabletLocationInfo> newLocationList = tabletArgs.getTabletLocationsList()
          .stream().map(OmTabletLocationInfo::getFromProtobuf)
          .collect(Collectors.toList());
      omTabletInfo.appendNewBlocks(newLocationList, false);

      omPartitionInfo = getPartitionInfo(omMetadataManager, databaseName, tableName, partitionName);
      // Here we refer to the implementation of HDFS:
      // If the key size is 600MB, when createKey, keyLocationInfo in
      // keyLocationList is 3, and  the every pre-allocated block length is
      // 256MB. If the number of factor  is 3, the total pre-allocated block
      // ize is 256MB * 3 * 3. We will allocate more 256MB * 3 * 3 - 600mb * 3
      // = 504MB in advance, and we  will subtract this part when we finally
      // commitKey.
      long preAllocatedSpace = newLocationList.size()
          * ozoneManager.getScmBlockSize()
          * omTabletInfo.getFactor().getNumber();

      // check table and database quota
      OmDatabaseArgs omDatabaseArgs = getDatabaseInfo(omMetadataManager, databaseName);
      OmTableInfo omTableInfo = getTableInfo(omMetadataManager, databaseName, tableName);
      checkTableQuotaInDatabase(omDatabaseArgs, omTableInfo, preAllocatedSpace);

      // Add to cache entry can be done outside of lock for this openKey.
      // Even if bucket gets deleted, when commitKey we shall identify if
      // bucket gets deleted.
      omMetadataManager.getOpenTabletTable().addCacheEntry(
          new CacheKey<>(dbOpenTabletName),
          new CacheValue<>(Optional.of(omTabletInfo), trxnLogIndex));

      // TODO: Update partition usedBytes && ?? Update table usedBytes
      omPartitionInfo.incrUsedBytes(preAllocatedSpace);
      // Update database quota
      omDatabaseArgs.incrUsedNamespace(1L);

      // Prepare response
      omResponse.setCreateTabletResponse(CreateTabletResponse.newBuilder()
          .setTabletInfo(omTabletInfo.getProtobuf(getOmRequest().getVersion()))
          .setID(clientID)
          .setOpenVersion(openVersion).build())
          .setCmdType(Type.CreateTablet);
      omClientResponse = new OMTabletCreateResponse(omResponse.build(),
          omTabletInfo, missingParentInfos, clientID, omPartitionInfo.copyObject(),
          omDatabaseArgs.copyObject());

      result = Result.SUCCESS;
    } catch (IOException ex) {
      result = Result.FAILURE;
      exception = ex;
      omMetrics.incNumKeyAllocateFails();
      omResponse.setCmdType(Type.CreateTablet);
      omClientResponse = new OMTabletCreateResponse(
          createErrorOMResponse(omResponse, exception));
    } finally {
      addResponseToDoubleBuffer(trxnLogIndex, omClientResponse,
          omDoubleBufferHelper);
      if (acquireLock) {
        omMetadataManager.getLock().releaseWriteLock(PARTITION_LOCK, databaseName,
            tableName, partitionName);
      }
    }

    // Audit Log outside the lock
    auditLog(ozoneManager.getAuditLogger(), buildAuditMessage(
        OMAction.ALLOCATE_TABLET, auditMap, exception,
        getOmRequest().getUserInfo()));

    switch (result) {
    case SUCCESS:
      // Missing directories are created immediately, counting that here.
      // The metric for the tablet is incremented as part of the tablet commit.
      omMetrics.incNumTablets(numMissingParents);
      LOG.debug("Tablet created. Database:{}, Table:{}, Partition:{}, Tablet: {}", databaseName,
          tableName, partitionName, tabletName);
      break;
    case FAILURE:
      LOG.error("Tablet ceation failed. Database:{}, Table:{}, Partition:{}, Tablet:{}." +
          "Exception:{}", databaseName, tableName, partitionName, tabletName, exception);
      break;
    default:
      LOG.error("Unrecognized Result for OMTabletCreateRequest: {}",
          createTabletRequest);
    }

    return omClientResponse;
  }

}
