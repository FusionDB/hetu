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
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AllocateTabletResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.util.Time;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests OMAllocateTabletResponse.
 */
public class TestOMAllocateTabletResponse extends TestOMTabletResponse {

  @Test
  public void testAddToDBBatch() throws Exception {

    OmTabletInfo omTabletInfo = TestOMRequestUtils.createOmTabletInfo(databaseName,
        tableName, partitionName, tabletName, replicationType, replicationFactor);
    OmPartitionInfo omPartitionInfo = TestOMResponseUtils.createPartition(databaseName, tableName,
        partitionName, "20200120");

    OMResponse omResponse = OMResponse.newBuilder()
        .setAllocateTabletResponse(
            AllocateTabletResponse.getDefaultInstance())
        .setStatus(OzoneManagerProtocolProtos.Status.OK)
        .setCmdType(OzoneManagerProtocolProtos.Type.AllocateBlock)
        .build();
    OMAllocateTabletResponse omAllocateTabletResponse =
        new OMAllocateTabletResponse(omResponse, omTabletInfo, clientID,
            omPartitionInfo);

    String openTablet = omMetadataManager.getOpenTablet(databaseName, tableName, partitionName,
        tabletName, clientID);

    // Not adding tablet entry before to test whether commit is successful or not.
    Assert.assertFalse(omMetadataManager.getOpenTabletTable().isExist(openTablet));
    omAllocateTabletResponse.addToDBBatch(omMetadataManager, batchOperation);

    // Do manual commit and see whether addToBatch is successful or not.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    Assert.assertTrue(omMetadataManager.getOpenTabletTable().isExist(openTablet));
  }

  @Test
  public void testAddToDBBatchWithErrorResponse() throws Exception {
    OmTabletInfo omTabletInfo = TestOMRequestUtils.createOmTabletInfo(databaseName, tableName,
        partitionName, tabletName, replicationType, replicationFactor);
    OmPartitionInfo omPartitionInfo = TestOMResponseUtils.createPartition(databaseName, tableName,
            partitionName, "20200120");

    OMResponse omResponse = OMResponse.newBuilder()
        .setAllocateTabletResponse(
            AllocateTabletResponse.getDefaultInstance())
        .setStatus(OzoneManagerProtocolProtos.Status.TABLET_NOT_FOUND)
        .setCmdType(OzoneManagerProtocolProtos.Type.AllocateTablet)
        .build();
    OMAllocateTabletResponse omAllocateTabletResponse =
        new OMAllocateTabletResponse(omResponse, omTabletInfo, clientID,
            omPartitionInfo);

    // Before calling addToDBBatch
    String openTablet = omMetadataManager.getOpenTablet(databaseName, tableName, partitionName,
        tabletName, clientID);
    Assert.assertFalse(omMetadataManager.getOpenTabletTable().isExist(openTablet));

    omAllocateTabletResponse.checkAndUpdateDB(omMetadataManager, batchOperation);

    // Do manual commit and see whether addToBatch is successful or not.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    // As omResponse is error it is a no-op.
    Assert.assertFalse(omMetadataManager.getOpenTabletTable().isExist(openTablet));

  }
}
