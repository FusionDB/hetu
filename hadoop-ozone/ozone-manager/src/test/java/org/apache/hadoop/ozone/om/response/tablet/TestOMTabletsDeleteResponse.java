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
import org.apache.hadoop.ozone.om.helpers.RepeatedOmTabletInfo;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.TestOMResponseUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteTabletsResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.util.Time;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.RATIS;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.OK;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.TABLET_NOT_FOUND;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.DeleteTablets;

/**
 * Class to test OMTabletsDeleteResponse.
 */
public class TestOMTabletsDeleteResponse extends TestOMTabletResponse {


  private List<OmTabletInfo> omTabletInfoList;
  private List<String> ozoneTablets;


  private void createPreRequisities() throws Exception {
    String parent = "db1/tab1/pt_20100120";
    String tablet = "tablet";

    omTabletInfoList = new ArrayList<>();
    ozoneTablets = new ArrayList<>();
    String ozoneTablet = "";
    for (int i = 0; i < 10; i++) {
      tabletName = parent.concat("/" + tablet + i);
      TestOMRequestUtils.addTabletToTable(false, databaseName,
          tableName, partitionName, tabletName, 0L, RATIS, THREE, omMetadataManager);
      ozoneTablet = omMetadataManager.getOzoneTablet(databaseName, tableName, partitionName, tabletName);
      omTabletInfoList.add(omMetadataManager.getTabletTable().get(ozoneTablet));
      ozoneTablets.add(ozoneTablet);
    }
  }

  @Test
  public void testTabletsDeleteResponse() throws Exception {

    createPreRequisities();

    OMResponse omResponse =
        OMResponse.newBuilder().setCmdType(DeleteTablets).setStatus(OK)
            .setSuccess(true)
            .setDeleteTabletsResponse(DeleteTabletsResponse.newBuilder()
                .setStatus(true)).build();

    OmPartitionInfo  omPartitionInfo = TestOMResponseUtils.createPartition(databaseName, tableName, partitionName, "20100120");

    OMClientResponse omTabletsDeleteResponse = new OMTabletsDeleteResponse(
        omResponse, omTabletInfoList, true, omPartitionInfo);

    omTabletsDeleteResponse.checkAndUpdateDB(omMetadataManager, batchOperation);

    omMetadataManager.getStore().commitBatchOperation(batchOperation);
    for (String ozTablet : ozoneTablets) {
      Assert.assertNull(omMetadataManager.getTabletTable().get(ozTablet));

      // ozTablet had no block information associated with it, so it should have
      // been removed from the tablet table but not added to the delete tablet.
      RepeatedOmTabletInfo repeatedOmTabletInfo =
          omMetadataManager.getDeletedTablet().get(ozTablet);
      Assert.assertNull(repeatedOmTabletInfo);
    }

  }

  @Test
  public void testTabletsDeleteResponseFail() throws Exception {
    createPreRequisities();

    OMResponse omResponse =
        OMResponse.newBuilder().setCmdType(DeleteTablets).setStatus(TABLET_NOT_FOUND)
            .setSuccess(false)
            .setDeleteTabletsResponse(DeleteTabletsResponse.newBuilder()
                .setStatus(false)).build();

    OmPartitionInfo  omPartitionInfo = TestOMResponseUtils.createPartition(databaseName, tableName, partitionName, "20100120");

    OMClientResponse omTabletsDeleteResponse = new OMTabletsDeleteResponse(
        omResponse, omTabletInfoList, true, omPartitionInfo);

    omTabletsDeleteResponse.checkAndUpdateDB(omMetadataManager, batchOperation);

    for (String ozTablet : ozoneTablets) {
      Assert.assertNotNull(omMetadataManager.getTabletTable().get(ozTablet));

      RepeatedOmTabletInfo repeatedOmTabletInfo =
          omMetadataManager.getDeletedTablet().get(ozTablet);
      Assert.assertNull(repeatedOmTabletInfo);

    }

  }
}
