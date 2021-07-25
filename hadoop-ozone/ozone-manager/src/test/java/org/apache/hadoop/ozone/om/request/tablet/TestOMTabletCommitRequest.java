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

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.helpers.OmTabletInfo;
import org.apache.hadoop.ozone.om.helpers.OmTabletLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmTabletInfo;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.om.request.tablet.OMTabletCommitRequest;
import org.apache.hadoop.ozone.om.request.tablet.TestOMTabletRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CommitTabletRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TabletArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TabletLocation;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Class tests OMKeyCommitRequest class.
 */
public class TestOMTabletCommitRequest extends TestOMTabletRequest {

  @Test
  public void testPreExecute() throws Exception {
    doPreExecute(createCommitTabletRequest());
  }

  @Test
  public void testValidateAndUpdateCache() throws Exception {

    OMRequest modifiedOmRequest =
        doPreExecute(createCommitTabletRequest());

    OMTabletCommitRequest omTabletCommitRequest =
        new OMTabletCommitRequest(modifiedOmRequest);

    TestOMRequestUtils.addDatabaseAndTableToDB(databaseName, tableName, omMetadataManager);
    TestOMRequestUtils.addPartitionToDB(databaseName, tableName, partitionName, omMetadataManager);

    TestOMRequestUtils.addTabletToTable(true, databaseName, tableName, partitionName, tabletName,
        clientID, replicationType, replicationFactor, omMetadataManager);

    String ozoneTablet = omMetadataManager.getOzoneTablet(databaseName, tableName,
        partitionName, tabletName);

    // Tablet should not be there in tablet table, as validateAndUpdateCache is
    // still not called.
    OmTabletInfo omTabletInfo = omMetadataManager.getTabletTable().get(ozoneTablet);

    Assert.assertNull(omTabletInfo);

    OMClientResponse omClientResponse =
        omTabletCommitRequest.validateAndUpdateCache(ozoneManager,
        100L, ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());

    // Entry should be deleted from openTablet Table.
    omTabletInfo = omMetadataManager.getOpenTabletTable().get(ozoneTablet);
    Assert.assertNull(omTabletInfo);

    // Now entry should be created in tablet Table.
    omTabletInfo = omMetadataManager.getTabletTable().get(ozoneTablet);

    Assert.assertNotNull(omTabletInfo);

    // Check modification time

    CommitTabletRequest commitTabletRequest = modifiedOmRequest.getCommitTabletRequest();
    Assert.assertEquals(commitTabletRequest.getTabletArgs().getModificationTime(),
        omTabletInfo.getModificationTime());

    // Check block location.
    List<OmTabletLocationInfo> locationInfoListFromCommitTabletRequest =
        commitTabletRequest.getTabletArgs()
        .getTabletLocationsList().stream().map(OmTabletLocationInfo::getFromProtobuf)
        .collect(Collectors.toList());

    Assert.assertEquals(locationInfoListFromCommitTabletRequest,
        omTabletInfo.getLatestVersionLocations().getLocationList());

  }

  @Test
  public void testValidateAndUpdateCacheWithDatabaseNotFound() throws Exception {

    OMRequest modifiedOmRequest =
        doPreExecute(createCommitTabletRequest());

    OMTabletCommitRequest omTabletCommitRequest =
        new OMTabletCommitRequest(modifiedOmRequest);

    String ozoneTablet = omMetadataManager.getOzoneTablet(databaseName, tableName, partitionName,
        tabletName);

    // Tablet should not be there in tablet table, as validateAndUpdateCache is
    // still not called.
    OmTabletInfo omTabletInfo = omMetadataManager.getTabletTable().get(ozoneTablet);

    Assert.assertNull(omTabletInfo);

    OMClientResponse omClientResponse =
        omTabletCommitRequest.validateAndUpdateCache(ozoneManager,
            100L, ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.DATABASE_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());

    omTabletInfo = omMetadataManager.getTabletTable().get(ozoneTablet);

    Assert.assertNull(omTabletInfo);
  }

  @Test
  public void testValidateAndUpdateCacheWithTableNotFound() throws Exception {

    OMRequest modifiedOmRequest =
        doPreExecute(createCommitTabletRequest());

    OMTabletCommitRequest omTabletCommitRequest =
        new OMTabletCommitRequest(modifiedOmRequest);


    TestOMRequestUtils.addDatabaseToDB(databaseName, OzoneConsts.OZONE,
        omMetadataManager);
    String ozoneTablet = omMetadataManager.getOzoneTablet(databaseName, tabletName,
        partitionName, tabletName);

    // Tablet should not be there in tablet table, as validateAndUpdateCache is
    // still not called.
    OmTabletInfo omTabletInfo = omMetadataManager.getTabletTable().get(ozoneTablet);

    Assert.assertNull(omTabletInfo);

    OMClientResponse omClientResponse =
        omTabletCommitRequest.validateAndUpdateCache(ozoneManager,
            100L, ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.TABLE_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());

    omTabletInfo = omMetadataManager.getTabletTable().get(ozoneTablet);

    Assert.assertNull(omTabletInfo);
  }

  @Test
  public void testValidateAndUpdateCacheWithPartitionNotFound() throws Exception {

    OMRequest modifiedOmRequest =
        doPreExecute(createCommitTabletRequest());

    OMTabletCommitRequest omTabletCommitRequest =
        new OMTabletCommitRequest(modifiedOmRequest);


    TestOMRequestUtils.addDatabaseAndTableToDB(databaseName, tableName,
        omMetadataManager);

    String ozoneTablet = omMetadataManager.getOzoneTablet(databaseName, tableName,
        partitionName, tabletName);

    // Tablet should not be there in tablet table, as validateAndUpdateCache is
    // still not called.
    OmTabletInfo omTabletInfo = omMetadataManager.getTabletTable().get(ozoneTablet);

    Assert.assertNull(omTabletInfo);

    OMClientResponse omClientResponse =
        omTabletCommitRequest.validateAndUpdateCache(ozoneManager,
            100L, ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.PARTITION_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());

    omTabletInfo = omMetadataManager.getTabletTable().get(ozoneTablet);

    Assert.assertNull(omTabletInfo);
  }

  /**
   * This method calls preExecute and verify the modified request.
   * @param originalOMRequest
   * @return OMRequest - modified request returned from preExecute.
   * @throws Exception
   */
  private OMRequest doPreExecute(OMRequest originalOMRequest) throws Exception {

    OMTabletCommitRequest omTabletCommitRequest =
        new OMTabletCommitRequest(originalOMRequest);

    OMRequest modifiedOmRequest = omTabletCommitRequest.preExecute(ozoneManager);

    Assert.assertTrue(modifiedOmRequest.hasCommitTabletRequest());
    TabletArgs originalTabletArgs =
        originalOMRequest.getCommitTabletRequest().getTabletArgs();
    TabletArgs modifiedTabletArgs =
        modifiedOmRequest.getCommitTabletRequest().getTabletArgs();
    verifyTabletArgs(originalTabletArgs, modifiedTabletArgs);
    return modifiedOmRequest;
  }

  /**
   * Verify TabletArgs.
   * @param originalTabletArgs
   * @param modifiedTabletArgs
   */
  private void verifyTabletArgs(TabletArgs originalTabletArgs, TabletArgs modifiedTabletArgs) {

    // Check modification time is set or not.
    Assert.assertTrue(modifiedTabletArgs.getModificationTime() > 0);
    Assert.assertTrue(originalTabletArgs.getModificationTime() == 0);

    Assert.assertEquals(originalTabletArgs.getDatabaseName(),
        modifiedTabletArgs.getDatabaseName());
    Assert.assertEquals(originalTabletArgs.getTableName(),
        modifiedTabletArgs.getTableName());
    Assert.assertEquals(originalTabletArgs.getPartitionName(),
        modifiedTabletArgs.getPartitionName());
    Assert.assertEquals(originalTabletArgs.getTabletName(),
            modifiedTabletArgs.getTabletName());
    Assert.assertEquals(originalTabletArgs.getDataSize(),
        modifiedTabletArgs.getDataSize());
    Assert.assertEquals(originalTabletArgs.getTabletLocationsList(),
        modifiedTabletArgs.getTabletLocationsList());
    Assert.assertEquals(originalTabletArgs.getType(),
        modifiedTabletArgs.getType());
    Assert.assertEquals(originalTabletArgs.getFactor(),
        modifiedTabletArgs.getFactor());
  }

  /**
   * Create OMRequest which encapsulates CommitTabletRequest.
   */
  private OMRequest createCommitTabletRequest() {
    TabletArgs tabletArgs =
        TabletArgs.newBuilder().setDataSize(dataSize).setDatabaseName(databaseName)
            .setTableName(tableName).setPartitionName(partitionName)
            .setTabletName(tabletName).setType(replicationType)
            .setFactor(replicationFactor)
            .addAllTabletLocations(getTabletLocation()).build();

    CommitTabletRequest commitTabletRequest =
        CommitTabletRequest.newBuilder().setTabletArgs(tabletArgs)
            .setClientID(clientID).build();

    return OMRequest.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.CommitTablet)
        .setCommitTabletRequest(commitTabletRequest)
        .setClientId(UUID.randomUUID().toString()).build();
  }

  /**
   * Create TabletLocation list.
   */
  private List<TabletLocation> getTabletLocation() {
    List<TabletLocation> tabletLocations = new ArrayList<>();

    for (int i=0; i < 5; i++) {
      TabletLocation tabletLocation =
          TabletLocation.newBuilder()
              .setBlockID(HddsProtos.BlockID.newBuilder()
                  .setContainerBlockID(HddsProtos.ContainerBlockID.newBuilder()
                      .setContainerID(i+1000).setLocalID(i+100).build()))
              .setOffset(0).setLength(200).build();
      tabletLocations.add(tabletLocation);
    }
    return tabletLocations;
  }

}
