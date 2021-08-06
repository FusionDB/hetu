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

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hetu.om.request.TestOMRequestUtils;
import org.apache.hadoop.hetu.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteTabletArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteTabletsRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.PARTIAL_DELETE;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.DeleteTablets;

/**
 * Class tests OMTabletsDeleteRequest.
 */
public class TestOMTabletsDeleteRequest extends TestOMTabletRequest {


  private List<String> deleteTabletList;
  private OMRequest omRequest;

  @Test
  public void testTabletsDeleteRequest() throws Exception {

    createPreRequisites();

    OMTabletsDeleteRequest omTabletsDeleteRequest =
        new OMTabletsDeleteRequest(omRequest);

    OMClientResponse omClientResponse =
        omTabletsDeleteRequest.validateAndUpdateCache(ozoneManager, 0L,
            ozoneManagerDoubleBufferHelper);

    Assert.assertTrue(omClientResponse.getOMResponse().getSuccess());
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());

    Assert.assertTrue(omClientResponse.getOMResponse().getDeleteTabletsResponse()
        .getStatus());
    DeleteTabletArgs unDeletedTablets =
        omClientResponse.getOMResponse().getDeleteTabletsResponse()
            .getUnDeletedTablets();
    Assert.assertEquals(0,
        unDeletedTablets.getTabletsCount());

    // Check all tablets are deleted.
    for (String deleteTablet : deleteTabletList) {
      Assert.assertNull(omMetadataManager.getTabletTable()
          .get(omMetadataManager.getOzoneTablet(databaseName, tableName, partitionName,
              deleteTablet)));
    }

  }

  @Test
  public void testTabletsDeleteRequestFail() throws Exception {

    createPreRequisites();

    // Add a tablet which not exist, which causes batch delete to fail.

    omRequest = omRequest.toBuilder()
            .setDeleteTabletsRequest(DeleteTabletsRequest.newBuilder()
                .setDeleteTablets(DeleteTabletArgs.newBuilder()
                .setDatabaseName(databaseName)
                .setTableName(tableName)
                .setPartitionName(partitionName)
                .addAllTablets(deleteTabletList).addTablets("dummy"))).build();

    OMTabletsDeleteRequest omTabletsDeleteRequest =
        new OMTabletsDeleteRequest(omRequest);

    OMClientResponse omClientResponse =
        omTabletsDeleteRequest.validateAndUpdateCache(ozoneManager, 0L,
        ozoneManagerDoubleBufferHelper);

    Assert.assertFalse(omClientResponse.getOMResponse().getSuccess());
    Assert.assertEquals(PARTIAL_DELETE,
        omClientResponse.getOMResponse().getStatus());

    Assert.assertFalse(omClientResponse.getOMResponse().getDeleteTabletsResponse()
        .getStatus());

    // Check tablets are deleted and in response check unDeletedTablet.
    for (String deleteTablet : deleteTabletList) {
      Assert.assertNull(omMetadataManager.getTabletTable()
          .get(omMetadataManager.getOzoneTablet(databaseName, tableName, partitionName,
              deleteTablet)));
    }

    DeleteTabletArgs unDeletedTablets = omClientResponse.getOMResponse()
        .getDeleteTabletsResponse().getUnDeletedTablets();
    Assert.assertEquals(1,
        unDeletedTablets.getTabletsCount());
    Assert.assertEquals("dummy", unDeletedTablets.getTablets(0));

  }

  private void createPreRequisites() throws Exception {

    deleteTabletList = new ArrayList<>();
    // Add database, table, partition and tablet entries to OM DB.
    TestOMRequestUtils.addDatabaseAndTableToDB(databaseName, tableName,
        omMetadataManager);
    TestOMRequestUtils.addPartitionToDB(databaseName, tableName, partitionName,
        omMetadataManager);

    int count = 10;

    DeleteTabletArgs.Builder deleteTabletArgs = DeleteTabletArgs.newBuilder()
        .setDatabaseName(databaseName)
        .setTableName(tableName)
        .setPartitionName(partitionName);

    // Create 10 tablets
    String parentDir = "/db1/tab1/pt_20100120";
    String tablet = "";


    for (int i = 0; i < count; i++) {
      tablet = parentDir.concat("/tablet" + i);
      TestOMRequestUtils.addTabletToTableCache(databaseName, tableName, partitionName,
          parentDir.concat("/tablet" + i), HddsProtos.ReplicationType.RATIS,
          HddsProtos.ReplicationFactor.THREE, omMetadataManager);
      deleteTabletArgs.addTablets(tablet);
      deleteTabletList.add(tablet);
    }

    omRequest =
        OMRequest.newBuilder().setClientId(UUID.randomUUID().toString())
            .setCmdType(DeleteTablets)
            .setDeleteTabletsRequest(DeleteTabletsRequest.newBuilder()
                .setDeleteTablets(deleteTabletArgs).build()).build();
  }

}
