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

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.ozone.om.helpers.OmPartitionInfo;
import org.apache.hadoop.ozone.om.helpers.OmTabletInfo;
import org.apache.hadoop.ozone.om.helpers.OmTabletLocationInfo;
import org.apache.hadoop.hetu.om.request.TestOMRequestUtils;
import org.apache.hadoop.hetu.om.response.TestOMResponseUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests OMTabletDeleteResponse.
 */
public class TestOMTabletDeleteResponse extends TestOMTabletResponse {

  @Test
  public void testAddToDBBatch() throws Exception {

    OmTabletInfo omTabletInfo = TestOMRequestUtils.createOmTabletInfo(databaseName,
        tableName, partitionName, tabletName, replicationType, replicationFactor);
    OmPartitionInfo omPartitionInfo = TestOMResponseUtils.createPartition(databaseName, tableName,
            partitionName, "202021");

    OzoneManagerProtocolProtos.OMResponse omResponse =
        OzoneManagerProtocolProtos.OMResponse.newBuilder().setDeleteTabletResponse(
            OzoneManagerProtocolProtos.DeleteTabletResponse.getDefaultInstance())
            .setStatus(OzoneManagerProtocolProtos.Status.OK)
            .setCmdType(OzoneManagerProtocolProtos.Type.DeleteTablet)
            .build();

    OMTabletDeleteResponse omTabletDeleteResponse = new OMTabletDeleteResponse(
        omResponse, omTabletInfo, true, omPartitionInfo);

    String ozoneTablet = omMetadataManager.getOzoneTablet(databaseName, tableName, partitionName,
        tabletName);

    TestOMRequestUtils.addTabletToTable(false, databaseName, tableName, partitionName, tabletName,
        clientID, replicationType, replicationFactor, omMetadataManager);

    Assert.assertTrue(omMetadataManager.getTabletTable().isExist(ozoneTablet));
    omTabletDeleteResponse.addToDBBatch(omMetadataManager, batchOperation);

    // Do manual commit and see whether addToBatch is successful or not.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    Assert.assertFalse(omMetadataManager.getTabletTable().isExist(ozoneTablet));

    // As default tablet entry does not have any blocks, it should not be in
    // deletedTabletTable.
    Assert.assertFalse(omMetadataManager.getDeletedTablet().isExist(
        ozoneTablet));
  }

  @Test
  public void testAddToDBBatchWithNonEmptyBlocks() throws Exception {

    OmTabletInfo omTabletInfo = TestOMRequestUtils.createOmTabletInfo(databaseName,
        tableName, partitionName, tabletName, replicationType, replicationFactor);
    OmPartitionInfo omPartitionInfo = TestOMResponseUtils.createPartition(databaseName, tableName,
            partitionName, "202021");

    // Add block to tablet.
    List<OmTabletLocationInfo> omTabletLocationInfoList = new ArrayList<>();

    Pipeline pipeline = Pipeline.newBuilder()
        .setState(Pipeline.PipelineState.OPEN)
        .setId(PipelineID.randomId())
        .setReplicationConfig(new RatisReplicationConfig(replicationFactor))
        .setNodes(new ArrayList<>())
        .build();

    OmTabletLocationInfo omTabletLocationInfo =
        new OmTabletLocationInfo.Builder().setBlockID(
            new BlockID(100L, 1000L))
            .setOffset(0).setLength(100L).setPipeline(pipeline).build();


    omTabletLocationInfoList.add(omTabletLocationInfo);

    omTabletInfo.appendNewBlocks(omTabletLocationInfoList, false);

    String ozoneTablet = omMetadataManager.getOzoneTablet(databaseName, tableName, partitionName,
        tabletName);

    omMetadataManager.getTabletTable().put(ozoneTablet, omTabletInfo);

    OzoneManagerProtocolProtos.OMResponse omResponse =
        OzoneManagerProtocolProtos.OMResponse.newBuilder().setDeleteTabletResponse(
            OzoneManagerProtocolProtos.DeleteTabletResponse.getDefaultInstance())
            .setStatus(OzoneManagerProtocolProtos.Status.OK)
            .setCmdType(OzoneManagerProtocolProtos.Type.DeleteTablet)
            .build();

    OMTabletDeleteResponse omTabletDeleteResponse = new OMTabletDeleteResponse(
        omResponse, omTabletInfo, true, omPartitionInfo);

    Assert.assertTrue(omMetadataManager.getTabletTable().isExist(ozoneTablet));
    omTabletDeleteResponse.addToDBBatch(omMetadataManager, batchOperation);

    // Do manual commit and see whether addToBatch is successful or not.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    Assert.assertFalse(omMetadataManager.getTabletTable().isExist(ozoneTablet));

    // Tablet has blocks, it should not be in deletedTabletTable.
    Assert.assertTrue(omMetadataManager.getDeletedTablet().isExist(
        ozoneTablet));
  }


  @Test
  public void testAddToDBBatchWithErrorResponse() throws Exception {
    OmTabletInfo omTabletInfo = TestOMRequestUtils.createOmTabletInfo(databaseName,
        tableName, partitionName, tabletName, replicationType, replicationFactor);

    OmPartitionInfo omPartitionInfo = TestOMResponseUtils.createPartition(databaseName, tableName,
            partitionName, "202021");

    OzoneManagerProtocolProtos.OMResponse omResponse =
        OzoneManagerProtocolProtos.OMResponse.newBuilder().setDeleteTabletResponse(
            OzoneManagerProtocolProtos.DeleteTabletResponse.getDefaultInstance())
            .setStatus(OzoneManagerProtocolProtos.Status.TABLET_NOT_FOUND)
            .setCmdType(OzoneManagerProtocolProtos.Type.DeleteTablet)
            .build();

    OMTabletDeleteResponse omTabletDeleteResponse = new OMTabletDeleteResponse(
        omResponse, omTabletInfo, true, omPartitionInfo);

    String ozoneTablet = omMetadataManager.getOzoneTablet(databaseName, tableName,
        partitionName, tabletName);

    TestOMRequestUtils.addTabletToTable(false, databaseName, tableName, partitionName, tabletName,
        clientID, replicationType, replicationFactor, omMetadataManager);

    Assert.assertTrue(omMetadataManager.getTabletTable().isExist(ozoneTablet));

    omTabletDeleteResponse.checkAndUpdateDB(omMetadataManager, batchOperation);

    // Do manual commit and see whether addToBatch is successful or not.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    // As omResponse is error it is a no-op. So, entry should be still in the
    // tabletTable.
    Assert.assertTrue(omMetadataManager.getTabletTable().isExist(ozoneTablet));

  }
}
