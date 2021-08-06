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

import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.hm.OmDatabaseArgs;
import org.apache.hadoop.ozone.om.helpers.OmTabletInfo;
import org.apache.hadoop.ozone.om.helpers.OmPartitionInfo;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.om.response.TestOMResponseUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateTabletResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.util.Time;
import org.junit.Assert;
import org.junit.Test;


/**
 * Tests OMTabletCreateResponse.
 */
public class TestOMTabletCreateResponse extends TestOMTabletResponse {

  @Test
  public void testAddToDBBatch() throws Exception {

    OmTabletInfo omTabletInfo = TestOMRequestUtils.createOmTabletInfo(databaseName,
        tableName, partitionName, tabletName, replicationType, replicationFactor);

    OmPartitionInfo omPartitionInfo = TestOMResponseUtils.createPartition(databaseName, tableName,
            partitionName, "202021");

    OmDatabaseArgs omDatabaseArgs = OmDatabaseArgs.newBuilder().setCreationTime(Time.now())
            .setName(databaseName)
            .setAdminName(OzoneConsts.ADMIN)
            .setOwnerName(OzoneConsts.OWNER)
            .setQuotaInBytes(Long.MAX_VALUE)
            .setQuotaInNamespace(10000L).build();

    OMResponse omResponse = OMResponse.newBuilder().setCreateTabletResponse(
                CreateTabletResponse.getDefaultInstance())
            .setStatus(OzoneManagerProtocolProtos.Status.OK)
            .setCmdType(OzoneManagerProtocolProtos.Type.CreateTablet)
            .build();

    OMTabletCreateResponse omTabletCreateResponse =
        new OMTabletCreateResponse(omResponse, omTabletInfo, null, clientID,
            omPartitionInfo, omDatabaseArgs);

    String openTablet = omMetadataManager.getOpenTablet(databaseName, tableName,
        partitionName, tabletName, clientID);
    Assert.assertFalse(omMetadataManager.getOpenTabletTable().isExist(openTablet));
    omTabletCreateResponse.addToDBBatch(omMetadataManager, batchOperation);

    // Do manual commit and see whether addToBatch is successful or not.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    Assert.assertTrue(omMetadataManager.getOpenTabletTable().isExist(openTablet));
  }

  @Test
  public void testAddToDBBatchWithErrorResponse() throws Exception {
    OmTabletInfo omTabletInfo = TestOMRequestUtils.createOmTabletInfo(databaseName,
        tableName, partitionName, tabletName, replicationType, replicationFactor);

    OmPartitionInfo omPartitionInfo = TestOMResponseUtils.createPartition(databaseName, tableName,
            partitionName, "202021");

    OmDatabaseArgs omDatabaseArgs = OmDatabaseArgs.newBuilder().setCreationTime(Time.now())
            .setName(databaseName)
            .setAdminName(OzoneConsts.ADMIN)
            .setOwnerName(OzoneConsts.OWNER)
            .setQuotaInBytes(Long.MAX_VALUE)
            .setQuotaInNamespace(10000L).build();

    OMResponse omResponse = OMResponse.newBuilder().setCreateTabletResponse(
        CreateTabletResponse.getDefaultInstance())
        .setStatus(OzoneManagerProtocolProtos.Status.TABLET_NOT_FOUND)
        .setCmdType(OzoneManagerProtocolProtos.Type.CreateTablet)
        .build();

    OMTabletCreateResponse omTabletCreateResponse =
        new OMTabletCreateResponse(omResponse, omTabletInfo, null, clientID,
            omPartitionInfo, omDatabaseArgs);

    // Before calling addToDBBatch
    String openTablet = omMetadataManager.getOpenTablet(databaseName, tableName,
        partitionName, tabletName, clientID);
    Assert.assertFalse(omMetadataManager.getOpenTabletTable().isExist(openTablet));

    omTabletCreateResponse.checkAndUpdateDB(omMetadataManager, batchOperation);

    // Do manual commit and see whether addToBatch is successful or not.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    // As omResponse is error it is a no-op.
    Assert.assertFalse(omMetadataManager.getOpenTabletTable().isExist(openTablet));

  }
}
