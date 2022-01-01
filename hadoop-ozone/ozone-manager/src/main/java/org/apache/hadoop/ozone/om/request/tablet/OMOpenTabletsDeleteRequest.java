/*
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
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OmTabletInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.tablet.OMOpenTabletsDeleteResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OpenTablet;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OpenTabletPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.PARTITION_LOCK;

/**
 * Handles requests to move open tablets from the open tablet table to the delete
 * tablet. Modifies the open tablet table cache only, and no underlying databases.
 * The delete table cache does not need to be modified since it is not used
 * for client response validation.
 */
public class OMOpenTabletsDeleteRequest extends OMTabletRequest {

  private static final Logger LOG =
          LoggerFactory.getLogger(OMOpenTabletsDeleteRequest.class);

  public OMOpenTabletsDeleteRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long trxnLogIndex, OzoneManagerDoubleBufferHelper omDoubleBufferHelper) {

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumOpenTabletDeleteRequests();

    OzoneManagerProtocolProtos.DeleteOpenTabletsRequest deleteOpenTabletsRequest =
            getOmRequest().getDeleteOpenTabletsRequest();

    List<OpenTabletPartition> submittedOpenTabletPartitions =
            deleteOpenTabletsRequest.getOpenTabletsPerPartitionList();

    long numSubmittedOpenTablets = 0;
    for (OpenTabletPartition tabletPartition: submittedOpenTabletPartitions) {
      numSubmittedOpenTablets += tabletPartition.getTabletsCount();
    }

    LOG.debug("{} open tablets submitted for deletion.", numSubmittedOpenTablets);
    omMetrics.incNumOpenTabletsSubmittedForDeletion(numSubmittedOpenTablets);

    OzoneManagerProtocolProtos.OMResponse.Builder omResponse =
            OmResponseUtil.getOMResponseBuilder(getOmRequest());

    IOException exception = null;
    OMClientResponse omClientResponse = null;
    Result result = null;
    Map<String, OmTabletInfo> deletedOpenTablets = new HashMap<>();

    try {
      for (OpenTabletPartition openTabletPartition: submittedOpenTabletPartitions) {
        // For each partition where keys will be deleted from,
        // get its partition lock and update the cache accordingly.
        Map<String, OmTabletInfo> deleted = updateOpenTabletTableCache(ozoneManager,
            trxnLogIndex, openTabletPartition);

        deletedOpenTablets.putAll(deleted);
      }

      omClientResponse = new OMOpenTabletsDeleteResponse(omResponse.build(),
          deletedOpenTablets, ozoneManager.isRatisEnabled());

      result = Result.SUCCESS;
    } catch (IOException ex) {
      result = Result.FAILURE;
      exception = ex;
      omClientResponse =
          new OMOpenTabletsDeleteResponse(createErrorOMResponse(omResponse,
              exception));
    } finally {
      addResponseToDoubleBuffer(trxnLogIndex, omClientResponse,
              omDoubleBufferHelper);
    }

    processResults(omMetrics, numSubmittedOpenTablets, deletedOpenTablets.size(),
        deleteOpenTabletsRequest, result);

    return omClientResponse;
  }

  private void processResults(OMMetrics omMetrics, long numSubmittedOpenTablets,
      long numDeletedOpenTablets,
      OzoneManagerProtocolProtos.DeleteOpenTabletsRequest request, Result result) {

    switch (result) {
    case SUCCESS:
      LOG.debug("Deleted {} open tablets out of {} submitted tablets.",
          numDeletedOpenTablets, numSubmittedOpenTablets);
      break;
    case FAILURE:
      omMetrics.incNumOpenTabletDeleteRequestFails();
      LOG.error("Failure occurred while trying to delete {} submitted open " +
              "tablets.", numSubmittedOpenTablets);
      break;
    default:
      LOG.error("Unrecognized result for OMOpenTabletsDeleteRequest: {}",
          request);
    }
  }

  private Map<String, OmTabletInfo> updateOpenTabletTableCache(
      OzoneManager ozoneManager, long trxnLogIndex, OpenTabletPartition tabletsPerPartition)
      throws IOException {

    Map<String, OmTabletInfo> deletedTablets = new HashMap<>();

    boolean acquiredLock = false;
    String databaseName = tabletsPerPartition.getDatabaseName();
    String tableName = tabletsPerPartition.getTableName();
    String partitionName = tabletsPerPartition.getPartitionName();
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    try {
      acquiredLock = omMetadataManager.getLock().acquireWriteLock(PARTITION_LOCK,
              databaseName, tableName, partitionName);

      for (OpenTablet tablet: tabletsPerPartition.getTabletsList()) {
        String fullTabletName = omMetadataManager.getOpenTablet(databaseName,
                tableName, partitionName, tablet.getName(), tablet.getClientID());

        // If an open tablet is no longer present in the table, it was committed
        // and should not be deleted.
        OmTabletInfo omTabletInfo =
            omMetadataManager.getOpenTabletTable().get(fullTabletName);
        if (omTabletInfo != null) {
          // Set the UpdateID to current transactionLogIndex
          omTabletInfo.setUpdateID(trxnLogIndex, ozoneManager.isRatisEnabled());
          deletedTablets.put(fullTabletName, omTabletInfo);

          // Update table cache.
          omMetadataManager.getOpenTabletTable().addCacheEntry(
                  new CacheKey<>(fullTabletName),
                  new CacheValue<>(Optional.absent(), trxnLogIndex));

          ozoneManager.getMetrics().incNumOpenTabletsDeleted();
          LOG.debug("Open tablet {} deleted.", fullTabletName);

          // No need to add cache entries to delete table. As delete table will
          // be used by DeleteTabletService only, not used for any client response
          // validation, so we don't need to add to cache.
        } else {
          LOG.debug("Tablet {} was not deleted, as it was not " +
                  "found in the open tablet table.", fullTabletName);
        }
      }
    } finally {
      if (acquiredLock) {
        omMetadataManager.getLock().releaseWriteLock(PARTITION_LOCK, databaseName,
                tableName, partitionName);
      }
    }

    return deletedTablets;
  }
}
