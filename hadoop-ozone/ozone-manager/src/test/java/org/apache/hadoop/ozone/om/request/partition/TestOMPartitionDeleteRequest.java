/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.ozone.om.request.partition;

import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeletePartitionRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

/**
 * Tests OMPartitionDeleteRequest class which handles DeletePartition request.
 */
public class TestOMPartitionDeleteRequest extends TestPartitionRequest {

  @Test
  public void testPreExecute() throws Exception {
    OMRequest omRequest =
        createDeletePartitionRequest(UUID.randomUUID().toString(),
            UUID.randomUUID().toString(), UUID.randomUUID().toString());

    OMPartitionDeleteRequest omPartitionDeleteRequest =
        new OMPartitionDeleteRequest(omRequest);

    // As user info gets added.
    Assert.assertNotEquals(omRequest,
        omPartitionDeleteRequest.preExecute(ozoneManager));
  }

  @Test
  public void testValidateAndUpdateCache() throws Exception {
    String databaseName = UUID.randomUUID().toString();
    String tableName = UUID.randomUUID().toString();
    String partitionName = UUID.randomUUID().toString();
    OMRequest omRequest =
        createDeletePartitionRequest(databaseName, tableName, partitionName);

    OMPartitionDeleteRequest omPartitionDeleteRequest =
        new OMPartitionDeleteRequest(omRequest);

    // Create database and table entries in DB.
    TestOMRequestUtils.addDatabaseAndTableToDB(databaseName, tableName,
        omMetadataManager);

    // Create partition entries in DB.
    TestOMRequestUtils.addPartitionToDB(databaseName, tableName, partitionName,
            omMetadataManager);

    omPartitionDeleteRequest.validateAndUpdateCache(ozoneManager, 1,
        ozoneManagerDoubleBufferHelper);

    Assert.assertNull(omMetadataManager.getPartitionTable().get(
        omMetadataManager.getPartitionKey(databaseName, tableName, partitionName)));
  }

  @Test
  public void testValidateAndUpdateCacheFailure() throws Exception {
    String databaseName = UUID.randomUUID().toString();
    String tableName = UUID.randomUUID().toString();
    String partitionName = UUID.randomUUID().toString();

    OMRequest omRequest =
        createDeletePartitionRequest(databaseName, tableName, partitionName);

    OMPartitionDeleteRequest omPartitionDeleteRequest =
        new OMPartitionDeleteRequest(omRequest);


    OMClientResponse omClientResponse =
        omPartitionDeleteRequest.validateAndUpdateCache(ozoneManager, 1,
            ozoneManagerDoubleBufferHelper);

    Assert.assertNull(omMetadataManager.getPartitionTable().get(
        omMetadataManager.getPartitionKey(databaseName, tableName, partitionName)));

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.PARTITION_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());

    // Create database and table entries in DB.
    TestOMRequestUtils.addDatabaseAndTableToDB(databaseName, tableName,
        omMetadataManager);

    // Create partition entries in DB.
    TestOMRequestUtils.addPartitionToDB(databaseName, tableName, partitionName,
            omMetadataManager);
  }

  private OMRequest createDeletePartitionRequest(String databaseName,
      String tableName, String partitionName) {
    return OMRequest.newBuilder().setDeletePartitionRequest(
        DeletePartitionRequest.newBuilder()
            .setTableName(tableName)
            .setDatabaseName(databaseName)
            .setPartitionName(partitionName))
        .setCmdType(OzoneManagerProtocolProtos.Type.DeletePartition)
        .setClientId(UUID.randomUUID().toString()).build();
  }
}
