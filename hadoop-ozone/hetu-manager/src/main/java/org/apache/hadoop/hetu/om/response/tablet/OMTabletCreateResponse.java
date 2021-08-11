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

package org.apache.hadoop.hetu.om.response.tablet;

import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hetu.hm.helpers.OmDatabaseArgs;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmPartitionInfo;
import org.apache.hadoop.ozone.om.helpers.OmTabletInfo;
import org.apache.hadoop.hetu.om.response.CleanupTableInfo;
import org.apache.hadoop.hetu.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.List;

import static org.apache.hadoop.hetu.om.OmMetadataManagerImpl.OPEN_TABLET_TABLE;
import static org.apache.hadoop.hetu.om.OmMetadataManagerImpl.TABLET_TABLE;

/**
 * Response for CreateTablet request.
 */
@CleanupTableInfo(cleanupTables = {OPEN_TABLET_TABLE, TABLET_TABLE})
public class OMTabletCreateResponse extends OMClientResponse {

  public static final Logger LOG =
      LoggerFactory.getLogger(OMTabletCreateResponse.class);
  private OmTabletInfo omTabletInfo;
  private long openTabletSessionID;
  private List<OmTabletInfo> parentTabletInfos;
  private OmPartitionInfo omPartitionInfo;
  private OmDatabaseArgs omDatabaseArgs;

  public OMTabletCreateResponse(@Nonnull OMResponse omResponse,
                                @Nonnull OmTabletInfo omTabletInfo, List<OmTabletInfo> parentTabletInfos,
                                long openTabletSessionID, @Nonnull OmPartitionInfo omPartitionInfo,
                                @Nonnull OmDatabaseArgs omDatabaseArgs) {
    super(omResponse);
    this.omTabletInfo = omTabletInfo;
    this.openTabletSessionID = openTabletSessionID;
    this.parentTabletInfos = parentTabletInfos;
    this.omPartitionInfo = omPartitionInfo;
    this.omDatabaseArgs = omDatabaseArgs;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public OMTabletCreateResponse(@Nonnull OMResponse omResponse) {
    super(omResponse);
    checkStatusNotOK();
  }

  @Override
  protected void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    /**
     * Create parent directory entries during tablet Create - do not wait
     * for Tablet Commit request.
     * XXX handle stale directory entries.
     */
    if (parentTabletInfos != null) {
      for (OmTabletInfo parentTabletInfo : parentTabletInfos) {
        String parentTablet = omMetadataManager
            .getOzoneDirTablet(parentTabletInfo.getDatabaseName(),
                parentTabletInfo.getTableName(), parentTabletInfo.getPartitionName(),
                parentTabletInfo.getTabletName());
        if (LOG.isDebugEnabled()) {
          LOG.debug("putWithBatch adding parent : tablet {} info : {}", parentTablet,
              parentTabletInfo);
        }
        omMetadataManager.getTabletTable()
            .putWithBatch(batchOperation, parentTablet, parentTabletInfo);
      }
    }

    String openTablet = omMetadataManager.getOpenTablet(omTabletInfo.getDatabaseName(),
        omTabletInfo.getTableName(), omTabletInfo.getPartitionName(),
        omTabletInfo.getTabletName(), openTabletSessionID);
    omMetadataManager.getOpenTabletTable().putWithBatch(batchOperation,
        openTablet, omTabletInfo);

    // update partition usedBytes.
    omMetadataManager.getPartitionTable().putWithBatch(batchOperation,
        omMetadataManager.getPartitionKey(omTabletInfo.getDatabaseName(),
            omTabletInfo.getTableName(), omTabletInfo.getPartitionName()), omPartitionInfo);

    // TODO update table usedBytes.

    // update database quota.
    omMetadataManager.getDatabaseTable().putWithBatch(batchOperation, omMetadataManager.getDatabaseKey(omTabletInfo.getDatabaseName()),
            omDatabaseArgs);
  }
}

