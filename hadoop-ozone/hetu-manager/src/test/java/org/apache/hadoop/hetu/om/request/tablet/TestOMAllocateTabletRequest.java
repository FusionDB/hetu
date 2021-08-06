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


import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.helpers.OmTabletInfo;
import org.apache.hadoop.ozone.om.helpers.OmTabletLocationInfo;
import org.apache.hadoop.hetu.om.request.TestOMRequestUtils;
import org.apache.hadoop.hetu.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AllocateTabletRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TabletArgs;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.UUID;

/**
 * Tests OMAllocateTabletRequest class.
 */
public class TestOMAllocateTabletRequest extends TestOMTabletRequest {

  @Test
  public void testPreExecute() throws Exception {

    doPreExecute(createAllocateTabletRequest());

  }

  @Test
  public void testValidateAndUpdateCache() throws Exception {
    // Add database, table, partition, tablet entries to DB.
    TestOMRequestUtils.addDatabaseAndTableToDB(databaseName, tableName,
        omMetadataManager);
    TestOMRequestUtils.addPartitionToDB(databaseName, tableName,
        partitionName, omMetadataManager);

    TestOMRequestUtils.addTabletToTable(true, databaseName, tableName, partitionName, tabletName,
        clientID, replicationType, replicationFactor, omMetadataManager);

    OMRequest modifiedOmRequest =
        doPreExecute(createAllocateTabletRequest());

    OMAllocateTabletRequest omAllocateTabletRequest =
        new OMAllocateTabletRequest(modifiedOmRequest);

    // Check before calling validateAndUpdateCache. As adding DB entry has
    // not added any tablet blocks, so size should be zero.

    OmTabletInfo omTabletInfo =
        omMetadataManager.getOpenTabletTable().get(omMetadataManager.getOpenTablet(
            databaseName, tableName, partitionName, tabletName, clientID));

    List<OmTabletLocationInfo> omTabletLocationInfo =
        omTabletInfo.getLatestVersionLocations().getLocationList();

    Assert.assertTrue(omTabletLocationInfo.size() == 0);

    OMClientResponse omAllocateTabletResponse =
        omAllocateTabletRequest.validateAndUpdateCache(ozoneManager, 100L,
            ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omAllocateTabletResponse.getOMResponse().getStatus());

    // Check open table whether new tablet block is added or not.

    omTabletInfo =
        omMetadataManager.getOpenTabletTable().get(omMetadataManager.getOpenTablet(
            databaseName, tableName, partitionName, tabletName, clientID));


    // Check modification time
    Assert.assertEquals(modifiedOmRequest.getAllocateTabletRequest()
        .getTabletArgs().getModificationTime(), omTabletInfo.getModificationTime());

    // creationTime was assigned at TestOMRequestUtils.addTabletToTable
    // modificationTime was assigned at
    // doPreExecute(createAllocateTabletRequest())
    Assert.assertTrue(
        omTabletInfo.getCreationTime() <= omTabletInfo.getModificationTime());

    // Check data of the tablet block
    OzoneManagerProtocolProtos.TabletLocation tabletLocation =
        modifiedOmRequest.getAllocateTabletRequest().getTabletLocation();

    omTabletLocationInfo =
        omTabletInfo.getLatestVersionLocations().getLocationList();

    Assert.assertTrue(omTabletLocationInfo.size() == 1);

    Assert.assertEquals(tabletLocation.getBlockID().getContainerBlockID()
        .getContainerID(), omTabletLocationInfo.get(0).getContainerID());

    Assert.assertEquals(tabletLocation.getBlockID().getContainerBlockID()
            .getLocalID(), omTabletLocationInfo.get(0).getLocalID());

  }

  @Test
  public void testValidateAndUpdateCacheWithDatabaseNotFound() throws Exception {

    OMRequest modifiedOmRequest =
        doPreExecute(createAllocateTabletRequest());

    OMAllocateTabletRequest omAllocateTabletRequest =
        new OMAllocateTabletRequest(modifiedOmRequest);


    OMClientResponse omAllocateTabletResponse =
        omAllocateTabletRequest.validateAndUpdateCache(ozoneManager, 100L,
            ozoneManagerDoubleBufferHelper);

    Assert.assertTrue(omAllocateTabletResponse.getOMResponse().getStatus()
        == OzoneManagerProtocolProtos.Status.DATABASE_NOT_FOUND);

  }

  @Test
  public void testValidateAndUpdateCacheWithTableNotFound() throws Exception {

    OMRequest modifiedOmRequest =
        doPreExecute(createAllocateTabletRequest());

    OMAllocateTabletRequest omAllocateTabletRequest =
        new OMAllocateTabletRequest(modifiedOmRequest);


    // Added only database to DB.
    TestOMRequestUtils.addDatabaseToDB(databaseName, OzoneConsts.OZONE,
        omMetadataManager);

    OMClientResponse omAllocateTabletResponse =
        omAllocateTabletRequest.validateAndUpdateCache(ozoneManager, 100L,
            ozoneManagerDoubleBufferHelper);

    Assert.assertTrue(omAllocateTabletResponse.getOMResponse().getStatus()
        == OzoneManagerProtocolProtos.Status.TABLE_NOT_FOUND);

  }

  @Test
  public void testValidateAndUpdateCacheWithPartitionNotFound() throws Exception {

    OMRequest modifiedOmRequest =
        doPreExecute(createAllocateTabletRequest());

    OMAllocateTabletRequest omAllocateTabletRequest =
        new OMAllocateTabletRequest(modifiedOmRequest);

    // Add database, table entries to DB.
    TestOMRequestUtils.addDatabaseAndTableToDB(databaseName, tableName,
            omMetadataManager);

    OMClientResponse omAllocateTabletResponse =
        omAllocateTabletRequest.validateAndUpdateCache(ozoneManager, 100L,
            ozoneManagerDoubleBufferHelper);

    Assert.assertTrue(omAllocateTabletResponse.getOMResponse().getStatus()
        == OzoneManagerProtocolProtos.Status.PARTITION_NOT_FOUND);

  }

  @Test
  public void testValidateAndUpdateCacheWithTabletNotFound() throws Exception {

    OMRequest modifiedOmRequest =
            doPreExecute(createAllocateTabletRequest());

    OMAllocateTabletRequest omAllocateTabletRequest =
            new OMAllocateTabletRequest(modifiedOmRequest);

    // Add database, table and partition entries to DB.
    TestOMRequestUtils.addDatabaseAndTableToDB(databaseName, tableName,
            omMetadataManager);
    TestOMRequestUtils.addPartitionToDB(databaseName, tableName,
            partitionName, omMetadataManager);

    OMClientResponse omAllocateTabletResponse =
            omAllocateTabletRequest.validateAndUpdateCache(ozoneManager, 100L,
                    ozoneManagerDoubleBufferHelper);

    Assert.assertTrue(omAllocateTabletResponse.getOMResponse().getStatus()
            == OzoneManagerProtocolProtos.Status.TABLET_NOT_FOUND);

  }

  /**
   * This method calls preExecute and verify the modified request.
   * @param originalOMRequest
   * @return OMRequest - modified request returned from preExecute.
   * @throws Exception
   */
  private OMRequest doPreExecute(OMRequest originalOMRequest) throws Exception {

    OMAllocateTabletRequest omAllocateTabletRequest =
        new OMAllocateTabletRequest(originalOMRequest);

    OMRequest modifiedOmRequest =
        omAllocateTabletRequest.preExecute(ozoneManager);


    Assert.assertEquals(originalOMRequest.getCmdType(),
        modifiedOmRequest.getCmdType());
    Assert.assertEquals(originalOMRequest.getClientId(),
        modifiedOmRequest.getClientId());

    Assert.assertTrue(modifiedOmRequest.hasAllocateTabletRequest());
    AllocateTabletRequest allocateTabletRequest =
        modifiedOmRequest.getAllocateTabletRequest();
    // Time should be set
    Assert.assertTrue(allocateTabletRequest.getTabletArgs()
        .getModificationTime() > 0);

    // TabletLocation should be set.
    Assert.assertTrue(allocateTabletRequest.hasTabletLocation());
    Assert.assertEquals(CONTAINER_ID,
        allocateTabletRequest.getTabletLocation().getBlockID()
            .getContainerBlockID().getContainerID());
    Assert.assertEquals(LOCAL_ID,
        allocateTabletRequest.getTabletLocation().getBlockID()
            .getContainerBlockID().getLocalID());
    Assert.assertTrue(allocateTabletRequest.getTabletLocation().hasPipeline());

    Assert.assertEquals(allocateTabletRequest.getClientID(),
        allocateTabletRequest.getClientID());

    return modifiedOmRequest;
  }


  private OMRequest createAllocateTabletRequest() {

    TabletArgs tabletArgs = TabletArgs.newBuilder()
        .setDatabaseName(databaseName).setTableName(tableName)
        .setPartitionName(partitionName).setTabletName(tabletName)
        .setFactor(replicationFactor).setType(replicationType)
        .build();

    AllocateTabletRequest allocateTabletRequest =
        AllocateTabletRequest.newBuilder().setClientID(clientID)
            .setTabletArgs(tabletArgs).build();

    return OMRequest.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.AllocateBlock)
        .setClientId(UUID.randomUUID().toString())
        .setAllocateTabletRequest(allocateTabletRequest).build();

  }
}
