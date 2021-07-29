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

package org.apache.hadoop.ozone.om.response.tablet;

import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmPartitionInfo;
import org.apache.hadoop.ozone.om.helpers.OmTabletInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.List;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.TABLET_TABLE;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.OK;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.PARTIAL_DELETE;

/**
 * Response for DeleteTablet request.
 */
@CleanupTableInfo(cleanupTables = TABLET_TABLE)
public class OMTabletsDeleteResponse extends AbstractOMTabletDeleteResponse {
  private List<OmTabletInfo> omTabletInfoList;
  private OmPartitionInfo omPartitionInfo;

  public OMTabletsDeleteResponse(@Nonnull OMResponse omResponse,
                                 @Nonnull List<OmTabletInfo> tabletDeleteList,
                                 boolean isRatisEnabled, @Nonnull OmPartitionInfo omPartitionInfo) {
    super(omResponse, isRatisEnabled);
    this.omTabletInfoList = tabletDeleteList;
    this.omPartitionInfo = omPartitionInfo;
  }

  /**
   * For when the request is not successful or it is a replay transaction.
   * For a successful request, the other constructor should be used.
   */
  public OMTabletsDeleteResponse(@Nonnull OMResponse omResponse) {
    super(omResponse);
  }

  @Override
  public void checkAndUpdateDB(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {
    if (getOMResponse().getStatus() == OK ||
        getOMResponse().getStatus() == PARTIAL_DELETE) {
      addToDBBatch(omMetadataManager, batchOperation);
    }
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
                           BatchOperation batchOperation) throws IOException {
    String databaseName = "";
    String tableName = "";
    String partitionName = "";
    String tabletName = "";
    Table<String, OmTabletInfo> tabletTable = omMetadataManager.getTabletTable();
    for (OmTabletInfo omTabletInfo : omTabletInfoList) {
      databaseName = omTabletInfo.getDatabaseName();
      tableName = omTabletInfo.getTableName();
      partitionName = omTabletInfo.getPartitionName();
      tabletName = omTabletInfo.getTabletName();

      String deleteTablet = omMetadataManager.getOzoneTablet(databaseName, tableName,
          partitionName, tabletName);

      addDeletionToBatch(omMetadataManager, batchOperation, tabletTable,
          deleteTablet, omTabletInfo);
    }

    // update partition usedBytes.
    omMetadataManager.getPartitionTable().putWithBatch(batchOperation,
        omMetadataManager.getPartitionKey(omPartitionInfo.getDatabaseName(),
            omPartitionInfo.getTableName(), omPartitionInfo.getPartitionName()), omPartitionInfo);

    // TODO update table usedBytes.
    // TODO update database usedByte & namespace quota in tablet numbers
  }
}