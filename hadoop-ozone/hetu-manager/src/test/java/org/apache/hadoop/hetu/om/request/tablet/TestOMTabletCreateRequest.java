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
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateTabletRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TabletArgs;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.UUID;

import static org.apache.hadoop.hetu.om.request.TestOMRequestUtils.addDatabaseAndTableToDB;
import static org.apache.hadoop.hetu.om.request.TestOMRequestUtils.addPartitionToDB;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.OK;

/**
 * Tests OMCreateTabletRequest class.
 */
public class TestOMTabletCreateRequest extends TestOMTabletRequest {

  @Test
  public void testPreExecuteWithNormalTablet() throws Exception {
    doPreExecute(createTabletRequest(false, 0));
  }

  @Test
  public void testPreExecuteWithMultipartTablet() throws Exception {
    doPreExecute(createTabletRequest(true, 1));
  }

  @Test
  public void testValidateAndUpdateCache() throws Exception {

    OMRequest modifiedOmRequest =
        doPreExecute(createTabletRequest(false, 0));

    OMTabletCreateRequest omTabletCreateRequest =
        new OMTabletCreateRequest(modifiedOmRequest);

    // Add database and table and partition entries to DB.
    addDatabaseAndTableToDB(databaseName, tableName,
        omMetadataManager);
    addPartitionToDB(databaseName, tableName, partitionName, omMetadataManager);

    long id = modifiedOmRequest.getCreateTabletRequest().getClientID();

    String openTablet = omMetadataManager.getOpenTablet(databaseName, tableName,
        partitionName, tabletName, id);

    // Before calling
    OmTabletInfo omTabletInfo = omMetadataManager.getOpenTabletTable().get(openTablet);

    Assert.assertNull(omTabletInfo);

    OMClientResponse omTabletCreateResponse =
        omTabletCreateRequest.validateAndUpdateCache(ozoneManager, 100L,
            ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OK,
        omTabletCreateResponse.getOMResponse().getStatus());

    // Check open table whether tablet is added or not.

    omTabletInfo = omMetadataManager.getOpenTabletTable().get(openTablet);

    Assert.assertNotNull(omTabletInfo);

    List<OmTabletLocationInfo> omTabletLocationInfoList =
        omTabletInfo.getLatestVersionLocations().getLocationList();
    Assert.assertTrue(omTabletLocationInfoList.size() == 1);

    OmTabletLocationInfo omTabletLocationInfo = omTabletLocationInfoList.get(0);

    // Check modification time
    Assert.assertEquals(modifiedOmRequest.getCreateTabletRequest()
        .getTabletArgs().getModificationTime(), omTabletInfo.getModificationTime());

    Assert.assertEquals(omTabletInfo.getModificationTime(),
        omTabletInfo.getCreationTime());


    // Check data of the block
    OzoneManagerProtocolProtos.TabletLocation tabletLocation =
        modifiedOmRequest.getCreateTabletRequest().getTabletArgs().getTabletLocations(0);

    Assert.assertEquals(tabletLocation.getBlockID().getContainerBlockID()
        .getContainerID(), omTabletLocationInfo.getContainerID());
    Assert.assertEquals(tabletLocation.getBlockID().getContainerBlockID()
        .getLocalID(), omTabletLocationInfo.getLocalID());

  }

  @Test
  public void testValidateAndUpdateCacheWithMultipartCreate()
      throws Exception {
    int partNumber = 1;
    OMRequest modifiedOmRequest =
        doPreExecute(createTabletRequest(true, partNumber));

    OMTabletCreateRequest omTabletCreateRequest =
        new OMTabletCreateRequest(modifiedOmRequest);

    // Add database and table and partition entries to DB.
    addDatabaseAndTableToDB(databaseName, tableName, omMetadataManager);
    addPartitionToDB(databaseName, tableName, partitionName, omMetadataManager);

    long id = modifiedOmRequest.getCreateTabletRequest().getClientID();

    String openTablet = omMetadataManager.getOpenTablet(databaseName, tableName,
        partitionName, tabletName, id);

    // Before calling
    OmTabletInfo omTabletInfo = omMetadataManager.getOpenTabletTable().get(openTablet);

    Assert.assertNull(omTabletInfo);

    OMClientResponse omTabletCreateResponse =
        omTabletCreateRequest.validateAndUpdateCache(ozoneManager, 100L,
            ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(
        OK,
        omTabletCreateResponse.getOMResponse().getStatus());

    // As we got error, no entry should be created in openTabletTable.

    omTabletInfo = omMetadataManager.getOpenTabletTable().get(openTablet);

    Assert.assertNotNull(omTabletInfo);
  }

  @Test
  public void testValidateAndUpdateCacheWithDatabaseNotFound() throws Exception {

    OMRequest modifiedOmRequest =
        doPreExecute(createTabletRequest(false, 0));

    OMTabletCreateRequest omTabletCreateRequest =
        new OMTabletCreateRequest(modifiedOmRequest);


    long id = modifiedOmRequest.getCreateTabletRequest().getClientID();

    String openTablet = omMetadataManager.getOpenTablet(databaseName, tableName,
        partitionName, tabletName, id);


    // Before calling
    OmTabletInfo omTabletInfo = omMetadataManager.getOpenTabletTable().get(openTablet);

    Assert.assertNull(omTabletInfo);

    OMClientResponse omTabletCreateResponse =
        omTabletCreateRequest.validateAndUpdateCache(ozoneManager, 100L,
            ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.DATABASE_NOT_FOUND,
        omTabletCreateResponse.getOMResponse().getStatus());


    // As We got an error, openTablet Table should not have entry.
    omTabletInfo = omMetadataManager.getOpenTabletTable().get(openTablet);

    Assert.assertNull(omTabletInfo);

  }


  @Test
  public void testValidateAndUpdateCacheWithTableNotFound() throws Exception {


    OMRequest modifiedOmRequest =
        doPreExecute(createTabletRequest(
            false, 0));

    OMTabletCreateRequest omTabletCreateRequest =
        new OMTabletCreateRequest(modifiedOmRequest);


    long id = modifiedOmRequest.getCreateTabletRequest().getClientID();

    String openTablet = omMetadataManager.getOpenTablet(databaseName, tableName,
        partitionName, tabletName, id);

    TestOMRequestUtils.addDatabaseToDB(databaseName, OzoneConsts.OZONE,
        omMetadataManager);

    // Before calling
    OmTabletInfo omTabletInfo = omMetadataManager.getOpenTabletTable().get(openTablet);

    Assert.assertNull(omTabletInfo);

    OMClientResponse omTabletCreateResponse =
        omTabletCreateRequest.validateAndUpdateCache(ozoneManager, 100L,
            ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.TABLE_NOT_FOUND,
        omTabletCreateResponse.getOMResponse().getStatus());


    // As We got an error, openTablet Table should not have entry.
    omTabletInfo = omMetadataManager.getOpenTabletTable().get(openTablet);

    Assert.assertNull(omTabletInfo);

  }



  /**
   * This method calls preExecute and verify the modified request.
   * @param originalOMRequest
   * @return OMRequest - modified request returned from preExecute.
   * @throws Exception
   */
  private OMRequest doPreExecute(OMRequest originalOMRequest) throws Exception {

    OMTabletCreateRequest omTabletCreateRequest =
        new OMTabletCreateRequest(originalOMRequest);

    OMRequest modifiedOmRequest =
        omTabletCreateRequest.preExecute(ozoneManager);

    Assert.assertEquals(originalOMRequest.getCmdType(),
        modifiedOmRequest.getCmdType());
    Assert.assertEquals(originalOMRequest.getClientId(),
        modifiedOmRequest.getClientId());

    Assert.assertTrue(modifiedOmRequest.hasCreateTabletRequest());

    CreateTabletRequest createTabletRequest =
        modifiedOmRequest.getCreateTabletRequest();

    TabletArgs tabletArgs = createTabletRequest.getTabletArgs();
    // Time should be set
    Assert.assertTrue(tabletArgs.getModificationTime() > 0);


    // Client ID should be set.
    Assert.assertTrue(createTabletRequest.hasClientID());
    Assert.assertTrue(createTabletRequest.getClientID() > 0);


    if (!originalOMRequest.getCreateTabletRequest().getTabletArgs()
        .getIsMultipartTablet()) {

      // As our data size is 100, and scmBlockSize is default to 1000, so we
      // shall have only one block.
      List< OzoneManagerProtocolProtos.TabletLocation> tabletLocations =
          tabletArgs.getTabletLocationsList();
      // TabletLocation should be set.
      Assert.assertTrue(tabletLocations.size() == 1);
      Assert.assertEquals(CONTAINER_ID,
          tabletLocations.get(0).getBlockID().getContainerBlockID()
              .getContainerID());
      Assert.assertEquals(LOCAL_ID,
          tabletLocations.get(0).getBlockID().getContainerBlockID()
              .getLocalID());
      Assert.assertTrue(tabletLocations.get(0).hasPipeline());

      Assert.assertEquals(0, tabletLocations.get(0).getOffset());

      Assert.assertEquals(scmBlockSize, tabletLocations.get(0).getLength());
    } else {
      // We don't create blocks for multipart tablet in createTablet preExecute.
      Assert.assertTrue(tabletArgs.getTabletLocationsList().size() == 0);
    }

    return modifiedOmRequest;

  }

  /**
   * Create OMRequest which encapsulates CreateTabletRequest.
   * @param isMultipartTablet
   * @param partNumber
   * @return OMRequest.
   */

  @SuppressWarnings("parameterNumber")
  private OMRequest createTabletRequest(boolean isMultipartTablet, int partNumber) {
    return createTabletRequest(isMultipartTablet, partNumber, tabletName);
  }

  private OMRequest createTabletRequest(boolean isMultipartTablet, int partNumber,
      String tabletName) {

    TabletArgs.Builder tabletArgs = TabletArgs.newBuilder()
        .setDatabaseName(databaseName)
        .setTableName(tableName)
        .setPartitionName(partitionName)
        .setTabletName(tabletName)
        .setIsMultipartTablet(isMultipartTablet)
        .setFactor(replicationFactor).setType(replicationType);

    if (isMultipartTablet) {
      tabletArgs.setDataSize(dataSize).setMultipartNumber(partNumber);
    }

    CreateTabletRequest createTabletRequest =
        CreateTabletRequest.newBuilder().setTabletArgs(tabletArgs).build();

    return OMRequest.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.CreateTablet)
        .setClientId(UUID.randomUUID().toString())
        .setCreateTabletRequest(createTabletRequest).build();
  }

}
