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
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmPartitionInfo;
import org.apache.hadoop.ozone.om.helpers.OmTabletInfo;
import org.apache.hadoop.hetu.om.response.CleanupTableInfo;
import org.apache.hadoop.hetu.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

import javax.annotation.Nonnull;
import java.io.IOException;

import static org.apache.hadoop.hetu.om.OmMetadataManagerImpl.OPEN_TABLET_TABLE;

/**
 * Response for AllocateTablet block request.
 */
@CleanupTableInfo(cleanupTables = {OPEN_TABLET_TABLE})
public class OMAllocateTabletResponse extends OMClientResponse {

  private OmTabletInfo omTabletInfo;
  private long clientID;
  private OmPartitionInfo omPartitionInfo;

  public OMAllocateTabletResponse(@Nonnull OMResponse omResponse,
                                  @Nonnull OmTabletInfo omTabletInfo, long clientID,
                                  @Nonnull OmPartitionInfo omPartitionInfo) {
    super(omResponse);
    this.omTabletInfo = omTabletInfo;
    this.clientID = clientID;
    this.omPartitionInfo = omPartitionInfo;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public OMAllocateTabletResponse(@Nonnull OMResponse omResponse) {
    super(omResponse);
    checkStatusNotOK();
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    String openTablet = omMetadataManager.getOpenTablet(omTabletInfo.getDatabaseName(),
        omTabletInfo.getTableName(), omTabletInfo.getPartitionName(), omTabletInfo.getTabletName(), clientID);
    omMetadataManager.getOpenTabletTable().putWithBatch(batchOperation, openTablet,
        omTabletInfo);

    // update partition usedBytes.
    omMetadataManager.getPartitionTable().putWithBatch(batchOperation,
        omMetadataManager.getPartitionKey(omTabletInfo.getDatabaseName(),
            omTabletInfo.getTableName(), omTabletInfo.getPartitionName()), omPartitionInfo);

    // TODO: update table & database usedBytes & quota
  }
}
