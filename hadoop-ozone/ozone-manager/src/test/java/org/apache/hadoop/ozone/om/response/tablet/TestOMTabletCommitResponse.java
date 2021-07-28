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

import org.apache.hadoop.ozone.om.helpers.OmPartitionInfo;
import org.apache.hadoop.ozone.om.helpers.OmTabletInfo;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.om.response.TestOMResponseUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests OMTabletCommitResponse.
 */
public class TestOMTabletCommitResponse extends TestOMTabletResponse {

  @Test
  public void testAddToDBBatch() throws Exception {

    OmTabletInfo omTabletInfo = TestOMRequestUtils.createOmTabletInfo(databaseName,
        tableName, partitionName, tabletName, replicationType, replicationFactor);

    OmPartitionInfo omPartitionInfo = TestOMResponseUtils.createPartition(databaseName, tableName,
            partitionName, "202021");

    OzoneManagerProtocolProtos.OMResponse omResponse =
        OzoneManagerProtocolProtos.OMResponse.newBuilder().setCommitTabletResponse(
            OzoneManagerProtocolProtos.CommitTabletResponse.getDefaultInstance())
            .setStatus(OzoneManagerProtocolProtos.Status.OK)
            .setCmdType(OzoneManagerProtocolProtos.Type.CommitTablet)
            .build();

    // As during commit tablet, entry will be already there in openTabletTable.
    // Adding it here.
    TestOMRequestUtils.addTabletToTable(true, databaseName, tableName, partitionName, tabletName,
        clientID, replicationType, replicationFactor, omMetadataManager);

    String openTablet = omMetadataManager.getOpenTablet(databaseName, tableName, partitionName,
        tabletName, clientID);
    Assert.assertTrue(omMetadataManager.getOpenTabletTable().isExist(openTablet));

    String ozoneTablet = omMetadataManager.getOzoneTablet(databaseName, tableName, partitionName,
        tabletName);
    OMTabletCommitResponse omTabletCommitResponse = new OMTabletCommitResponse(
        omResponse, omTabletInfo, ozoneTablet, openTablet, omPartitionInfo);

    omTabletCommitResponse.addToDBBatch(omMetadataManager, batchOperation);

    // Do manual commit and see whether addToBatch is successful or not.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    // When tablet commit tablet is deleted from openTablet table and added to tabletTable.
    Assert.assertFalse(omMetadataManager.getOpenTabletTable().isExist(openTablet));
    Assert.assertTrue(omMetadataManager.getTabletTable().isExist(
        omMetadataManager.getOzoneTablet(databaseName, tableName, partitionName, tabletName)));
  }

  @Test
  public void testAddToDBBatchNoOp() throws Exception {

    OmTabletInfo omTabletInfo = TestOMRequestUtils.createOmTabletInfo(databaseName,
        tableName, partitionName, tabletName, replicationType, replicationFactor);

    OmPartitionInfo omPartitionInfo = TestOMResponseUtils.createPartition(databaseName, tableName,
            partitionName, "202021");

    OzoneManagerProtocolProtos.OMResponse omResponse =
        OzoneManagerProtocolProtos.OMResponse.newBuilder().setCommitTabletResponse(
            OzoneManagerProtocolProtos.CommitTabletResponse.getDefaultInstance())
            .setStatus(OzoneManagerProtocolProtos.Status.TABLET_NOT_FOUND)
            .setCmdType(OzoneManagerProtocolProtos.Type.CommitTablet)
            .build();

    String openTablet = omMetadataManager.getOpenTablet(databaseName, tableName,
        partitionName, tabletName, clientID);
    String ozoneTablet = omMetadataManager.getOzoneTablet(databaseName, tableName,
        partitionName, tabletName);

    OMTabletCommitResponse omTabletCommitResponse = new OMTabletCommitResponse(
        omResponse, omTabletInfo, ozoneTablet, openTablet, omPartitionInfo);

    // As during commit Tablet, entry will be already there in openTabletTable.
    // Adding it here.
    TestOMRequestUtils.addTabletToTable(true, databaseName, tableName, partitionName, tabletName,
        clientID, replicationType, replicationFactor, omMetadataManager);

    Assert.assertTrue(omMetadataManager.getOpenTabletTable().isExist(openTablet));

    omTabletCommitResponse.checkAndUpdateDB(omMetadataManager, batchOperation);

    // Do manual commit and see whether addToBatch is successful or not.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    // As omResponse is error it is a no-op. So, entry should still be in
    // openTablet table.
    Assert.assertTrue(omMetadataManager.getOpenTabletTable().isExist(openTablet));
    Assert.assertFalse(omMetadataManager.getTabletTable().isExist(
        omMetadataManager.getOzoneTablet(databaseName, tableName, partitionName, tabletName)));
  }
}
