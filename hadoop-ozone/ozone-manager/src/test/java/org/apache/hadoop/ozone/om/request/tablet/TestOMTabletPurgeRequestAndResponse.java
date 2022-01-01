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

import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.om.response.tablet.OMTabletPurgeResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeletedTablets;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PurgeTabletsRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PurgeTabletsResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Tests {@link OMTabletPurgeRequest} and {@link OMTabletPurgeResponse}.
 */
public class TestOMTabletPurgeRequestAndResponse extends TestOMTabletRequest {

  private int numTablets = 10;

  /**
   * Creates database, tablet, partition and tablet entries and adds to OM DB and then
   * deletes these tablets to move them to deletedTablets table.
   */
  private List<String> createAndDeleteTablets(Integer trxnIndex, String partition)
      throws Exception {
    if (partition == null) {
      partition = partitionName;
    }
    // Add database, table, partition and tablet entries to OM DB.
    TestOMRequestUtils.addDatabaseAndTableToDB(databaseName, tableName,
        omMetadataManager);
    TestOMRequestUtils.addPartitionToDB(databaseName, tableName,
        partitionName, omMetadataManager);

    List<String> ozoneTabletNames = new ArrayList<>(numTablets);
    for (int i = 1; i <= numTablets; i++) {
      String tablet = tabletName + "-" + i;
      TestOMRequestUtils.addTabletToTable(false, false, databaseName, tableName, partitionName,
          tablet, clientID, replicationType, replicationFactor, trxnIndex++,
          omMetadataManager);
      ozoneTabletNames.add(omMetadataManager.getOzoneTablet(
          databaseName, tableName, partitionName, tablet));
    }

    List<String> deletedTabletNames = new ArrayList<>(numTablets);
    for (String ozoneTablet : ozoneTabletNames) {
      String deletedTabletName = TestOMRequestUtils.deleteTablet(
          ozoneTablet, omMetadataManager, trxnIndex++);
      deletedTabletNames.add(deletedTabletName);
    }

    return deletedTabletNames;
  }

  /**
   * Create OMRequest which encapsulates DeleteTabletRequest.
   * @return OMRequest
   */
  private OMRequest createPurgeTabletsRequest(List<String> deletedTablets) {
    DeletedTablets deletedTabletsInPartition = DeletedTablets.newBuilder()
        .setDatabaseName(databaseName)
        .setTableName(tableName)
        .setPartitionName(partitionName)
        .addAllTablets(deletedTablets)
        .build();
    PurgeTabletsRequest purgeTabletsRequest = PurgeTabletsRequest.newBuilder()
        .addDeletedTablets(deletedTabletsInPartition)
        .build();

    return OMRequest.newBuilder()
        .setPurgeTabletsRequest(purgeTabletsRequest)
        .setCmdType(Type.PurgeTablets)
        .setClientId(UUID.randomUUID().toString())
        .build();
  }

  private OMRequest preExecute(OMRequest originalOmRequest) throws IOException {
    OMTabletPurgeRequest omTabletPurgeRequest =
        new OMTabletPurgeRequest(originalOmRequest);

    OMRequest modifiedOmRequest = omTabletPurgeRequest.preExecute(ozoneManager);

    // Will not be equal, as UserInfo will be set.
    Assert.assertNotEquals(originalOmRequest, modifiedOmRequest);

    return modifiedOmRequest;
  }

  @Test
  public void testValidateAndUpdateCache() throws Exception {
    // Create and Delete tablets. The tablets should be moved to DeletedTablets table
    List<String> deletedTabletNames = createAndDeleteTablets(1, null);

    // The tablets should be present in the DeletedTablets table before purging
    for (String deletedTablet : deletedTabletNames) {
      Assert.assertTrue(omMetadataManager.getDeletedTablet().isExist(
          deletedTablet));
    }

    // Create PurgeTabletsRequest to purge the deleted tablets
    OMRequest omRequest = createPurgeTabletsRequest(deletedTabletNames);

    OMRequest preExecutedRequest = preExecute(omRequest);
    OMTabletPurgeRequest omTabletPurgeRequest =
        new OMTabletPurgeRequest(preExecutedRequest);

    omTabletPurgeRequest.validateAndUpdateCache(ozoneManager, 100L,
        ozoneManagerDoubleBufferHelper);

    OMResponse omResponse = OMResponse.newBuilder()
        .setPurgeTabletsResponse(PurgeTabletsResponse.getDefaultInstance())
        .setCmdType(Type.PurgeTablets)
        .setStatus(Status.OK)
        .build();

    try(BatchOperation batchOperation =
        omMetadataManager.getStore().initBatchOperation()) {

      OMTabletPurgeResponse omTabletPurgeResponse = new OMTabletPurgeResponse(
          omResponse, deletedTabletNames);
      omTabletPurgeResponse.addToDBBatch(omMetadataManager, batchOperation);

      // Do manual commit and see whether addToBatch is successful or not.
      omMetadataManager.getStore().commitBatchOperation(batchOperation);
    }

    // The tablets should not exist in the DeletedTablets table
    for (String deletedTablet : deletedTabletNames) {
      Assert.assertFalse(omMetadataManager.getDeletedTablet().isExist(
          deletedTablet));
    }
  }
}
