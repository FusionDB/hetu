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

import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.om.request.table.OMTableSetPropertyRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetPartitionPropertyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PartitionArgs;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.UUID;

import static org.apache.hadoop.ozone.OzoneConsts.GB;

/**
 * Tests OMPartitionSetPropertyRequest class which handles OMSetPartitionProperty
 * request.
 */
public class TestOMPartitionSetPropertyRequest extends TestPartitionRequest {

  @Test
  public void testPreExecute() throws Exception {
    String databaseName = UUID.randomUUID().toString();
    String tableName = UUID.randomUUID().toString();
    String partitionName = UUID.randomUUID().toString();

    OMRequest omRequest = createSetPartitionPropertyRequest(databaseName,
        tableName, partitionName,true, Long.MAX_VALUE);

    OMPartitionSetPropertyRequest omPartitionSetPropertyRequest =
        new OMPartitionSetPropertyRequest(omRequest);

    // As user info gets added.
    Assert.assertNotEquals(omRequest,
        omPartitionSetPropertyRequest.preExecute(ozoneManager));
  }

  @Test
  public void testValidateAndUpdateCache() throws Exception {

    String databaseName = UUID.randomUUID().toString();
    String tableName = UUID.randomUUID().toString();
    String partitionName = UUID.randomUUID().toString();

    OMRequest omRequest = createSetPartitionPropertyRequest(databaseName,
        tableName, partitionName,true, Long.MAX_VALUE);

    // Create with default TableInfo values
    TestOMRequestUtils.addDatabaseAndTableToDB(databaseName, tableName,
        omMetadataManager);

    // Create with default PartitionInfo values
    TestOMRequestUtils.addPartitionToDB(databaseName, tableName,
            partitionName, omMetadataManager);

    OMPartitionSetPropertyRequest omPartitionSetPropertyRequest =
        new OMPartitionSetPropertyRequest(omRequest);

    OMClientResponse omClientResponse =
        omPartitionSetPropertyRequest.validateAndUpdateCache(ozoneManager, 1,
            ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(true,
        omMetadataManager.getPartitionTable().get(
            omMetadataManager.getPartitionKey(databaseName, tableName, partitionName))
            .getIsVersionEnabled());

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());
  }

  @Test
  public void testValidateAndUpdateCacheFails() throws Exception {

    String databaseName = UUID.randomUUID().toString();
    String tableName = UUID.randomUUID().toString();
    String partitionName = UUID.randomUUID().toString();

    OMRequest omRequest = createSetPartitionPropertyRequest(databaseName,
        tableName, partitionName, true, Long.MAX_VALUE);

    OMPartitionSetPropertyRequest omPartitionSetPropertyRequest =
        new OMPartitionSetPropertyRequest(omRequest);

    OMClientResponse omClientResponse =
        omPartitionSetPropertyRequest.validateAndUpdateCache(ozoneManager, 1,
            ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.PARTITION_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());

    Assert.assertNull(omMetadataManager.getPartitionTable().get(
        omMetadataManager.getPartitionKey(databaseName, tableName, partitionName)));

  }

  private OMRequest createSetPartitionPropertyRequest(String databaseName,
      String tableName, String partitionName, boolean isVersionEnabled, long sizeInBytes) {

    return OMRequest.newBuilder().setSetPartitionPropertyRequest(
        SetPartitionPropertyRequest.newBuilder().setPartitionArgs(
            PartitionArgs.newBuilder().setPartitionName(partitionName)
                .setTableName(tableName)
                .setDatabaseName(databaseName)
                .setRows(0)
                .setSizeInBytes(sizeInBytes)
                .setStorageType(StorageType.SSD.toProto())
                .setNumReplicas(3)
                .setPartitionValue("20210221")
                .setIsVersionEnabled(isVersionEnabled).build()))
        .setCmdType(OzoneManagerProtocolProtos.Type.SetPartitionProperty)
        .setClientId(UUID.randomUUID().toString()).build();
  }

  @Test
  public void testValidateAndUpdateCacheWithQuota() throws Exception {

    String databaseName = UUID.randomUUID().toString();
    String tableName = UUID.randomUUID().toString();
    String partitionName = UUID.randomUUID().toString();

    TestOMRequestUtils.addDatabaseToDB(
        databaseName, omMetadataManager, 10 * GB);
    TestOMRequestUtils.addMetaTableToDB(
        databaseName, tableName, omMetadataManager, 8 * GB);
    TestOMRequestUtils.addPartitionToDB(databaseName, tableName, partitionName, omMetadataManager);

    OMRequest omRequest = createSetPartitionPropertyRequest(databaseName,
        tableName, partitionName, true, 20 * GB);

    OMPartitionSetPropertyRequest omPartitionSetPropertyRequest =
        new OMPartitionSetPropertyRequest(omRequest);
    int countException = 0;
    try {
      omPartitionSetPropertyRequest.validateAndUpdateCache(ozoneManager, 1,
          ozoneManagerDoubleBufferHelper);
    } catch (IllegalArgumentException ex) {
      countException++;
      GenericTestUtils.assertExceptionContains(
          "Total tables quota in this database should not be " +
              "greater than database quota", ex);
    }
    Assert.assertEquals(1, countException);
  }
}
