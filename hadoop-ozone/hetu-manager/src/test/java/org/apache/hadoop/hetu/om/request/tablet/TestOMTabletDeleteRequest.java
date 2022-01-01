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

import org.apache.hadoop.ozone.om.helpers.OmTabletInfo;
import org.apache.hadoop.hetu.om.request.TestOMRequestUtils;
import org.apache.hadoop.hetu.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteTabletRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TabletArgs;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

/**
 * Tests OmTabletDelete request.
 */
public class TestOMTabletDeleteRequest extends TestOMTabletRequest {

  @Test
  public void testPreExecute() throws Exception {
    doPreExecute(createDeleteTabletRequest());
  }

  @Test
  public void testValidateAndUpdateCache() throws Exception {
    OMRequest modifiedOmRequest =
        doPreExecute(createDeleteTabletRequest());

    OMTabletDeleteRequest omTabletDeleteRequest =
        new OMTabletDeleteRequest(modifiedOmRequest);

    // Add database, tablet, partition and tablet entries to OM DB.
    TestOMRequestUtils.addDatabaseAndTableToDB(databaseName, tableName,
        omMetadataManager);
    TestOMRequestUtils.addPartitionToDB(databaseName, tableName, partitionName, omMetadataManager);

    TestOMRequestUtils.addTabletToTable(false, databaseName, tableName, partitionName, tabletName,
        clientID, replicationType, replicationFactor, omMetadataManager);

    String ozoneTablet = omMetadataManager.getOzoneTablet(databaseName, tableName, partitionName,
        tabletName);

    OmTabletInfo omTabletInfo = omMetadataManager.getTabletTable().get(ozoneTablet);

    // As we added manually to tablet table.
    Assert.assertNotNull(omTabletInfo);

    OMClientResponse omClientResponse =
        omTabletDeleteRequest.validateAndUpdateCache(ozoneManager,
        100L, ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());
    // Now after calling validateAndUpdateCache, it should be deleted.

    omTabletInfo = omMetadataManager.getTabletTable().get(ozoneTablet);

    Assert.assertNull(omTabletInfo);
  }

  @Test
  public void testValidateAndUpdateCacheWithTabletNotFound() throws Exception {
    OMRequest modifiedOmRequest =
        doPreExecute(createDeleteTabletRequest());

    OMTabletDeleteRequest omTabletDeleteRequest =
        new OMTabletDeleteRequest(modifiedOmRequest);

    // Add only database, table and partition entry to DB.
    // In actual implementation we don't check for partition/table/database exists
    // during delete tablet.
    TestOMRequestUtils.addDatabaseAndTableToDB(databaseName, tableName,
            omMetadataManager);
    TestOMRequestUtils.addPartitionToDB(databaseName, tableName, partitionName, omMetadataManager);

    OMClientResponse omClientResponse =
        omTabletDeleteRequest.validateAndUpdateCache(ozoneManager,
            100L, ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.TABLET_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());
  }

  @Test
  public void testValidateAndUpdateCacheWithDatabaseNotFound() throws Exception {
    OMRequest modifiedOmRequest =
        doPreExecute(createDeleteTabletRequest());

    OMTabletDeleteRequest omTabletDeleteRequest =
        new OMTabletDeleteRequest(modifiedOmRequest);

    OMClientResponse omClientResponse =
        omTabletDeleteRequest.validateAndUpdateCache(ozoneManager,
            100L, ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.DATABASE_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());
  }

  @Test
  public void testValidateAndUpdateCacheWithTableNotFound() throws Exception {
    OMRequest modifiedOmRequest =
        doPreExecute(createDeleteTabletRequest());

    OMTabletDeleteRequest omTabletDeleteRequest =
        new OMTabletDeleteRequest(modifiedOmRequest);

    TestOMRequestUtils.addDatabaseToDB(databaseName, omMetadataManager);

    OMClientResponse omClientResponse =
        omTabletDeleteRequest.validateAndUpdateCache(ozoneManager,
            100L, ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.TABLE_NOT_FOUND,
            omClientResponse.getOMResponse().getStatus());
  }

  @Test
  public void testValidateAndUpdateCacheWithPartitionNotFound() throws Exception {
    OMRequest modifiedOmRequest =
            doPreExecute(createDeleteTabletRequest());

    OMTabletDeleteRequest omTabletDeleteRequest =
            new OMTabletDeleteRequest(modifiedOmRequest);

    TestOMRequestUtils.addDatabaseAndTableToDB(databaseName, tableName, omMetadataManager);

    OMClientResponse omClientResponse =
            omTabletDeleteRequest.validateAndUpdateCache(ozoneManager,
                    100L, ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.PARTITION_NOT_FOUND,
            omClientResponse.getOMResponse().getStatus());
  }

  /**
   * This method calls preExecute and verify the modified request.
   * @param originalOmRequest
   * @return OMRequest - modified request returned from preExecute.
   * @throws Exception
   */
  private OMRequest doPreExecute(OMRequest originalOmRequest) throws Exception {

    OMTabletDeleteRequest omTabletDeleteRequest =
        new OMTabletDeleteRequest(originalOmRequest);

    OMRequest modifiedOmRequest = omTabletDeleteRequest.preExecute(ozoneManager);

    // Will not be equal, as UserInfo will be set.
    Assert.assertNotEquals(originalOmRequest, modifiedOmRequest);

    return modifiedOmRequest;
  }

  /**
   * Create OMRequest which encapsulates DeleteTabletRequest.
   * @return OMRequest
   */
  private OMRequest createDeleteTabletRequest() {
    TabletArgs tabletArgs = TabletArgs.newBuilder().setDatabaseName(databaseName)
        .setTableName(tableName).setPartitionName(partitionName).setTabletName(tabletName).build();

    DeleteTabletRequest deleteTabletRequest =
        DeleteTabletRequest.newBuilder().setTabletArgs(tabletArgs).build();

    return OMRequest.newBuilder().setDeleteTabletRequest(deleteTabletRequest)
        .setCmdType(OzoneManagerProtocolProtos.Type.DeleteTablet)
        .setClientId(UUID.randomUUID().toString()).build();
  }
}
