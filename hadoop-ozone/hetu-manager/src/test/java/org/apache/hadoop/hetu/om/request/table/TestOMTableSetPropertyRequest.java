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

package org.apache.hadoop.hetu.om.request.table;

import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hetu.om.request.TestOMRequestUtils;
import org.apache.hadoop.hetu.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetTablePropertyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TableArgs;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.UUID;

import static org.apache.hadoop.hetu.om.request.TestOMRequestUtils.getSchema;
import static org.apache.hadoop.ozone.OzoneConsts.GB;

/**
 * Tests OMTableSetPropertyRequest class which handles OMSetTableProperty
 * request.
 */
public class TestOMTableSetPropertyRequest extends TestTableRequest {

  @Test
  public void testPreExecute() throws Exception {
    String databaseName = UUID.randomUUID().toString();
    String tableName = UUID.randomUUID().toString();

    OMRequest omRequest = createSetTablePropertyRequest(databaseName,
        tableName, true, Long.MAX_VALUE);

    OMTableSetPropertyRequest omTableSetPropertyRequest =
        new OMTableSetPropertyRequest(omRequest);

    // As user info gets added.
    Assert.assertNotEquals(omRequest,
        omTableSetPropertyRequest.preExecute(ozoneManager));
  }

  @Test
  public void testValidateAndUpdateCache() throws Exception {

    String databaseName = UUID.randomUUID().toString();
    String tableName = UUID.randomUUID().toString();

    OMRequest omRequest = createSetTablePropertyRequest(databaseName,
        tableName, true, Long.MAX_VALUE);

    // Create with default TableInfo values
    TestOMRequestUtils.addDatabaseAndTableToDB(databaseName, tableName,
        omMetadataManager);

    OMTableSetPropertyRequest omTableSetPropertyRequest =
        new OMTableSetPropertyRequest(omRequest);

    OMClientResponse omClientResponse =
        omTableSetPropertyRequest.validateAndUpdateCache(ozoneManager, 1,
            ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(true,
        omMetadataManager.getMetaTable().get(
            omMetadataManager.getMetaTableKey(databaseName, tableName))
            .getIsVersionEnabled());

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());
  }

  @Test
  public void testValidateAndUpdateCacheFails() throws Exception {

    String databaseName = UUID.randomUUID().toString();
    String tableName = UUID.randomUUID().toString();

    OMRequest omRequest = createSetTablePropertyRequest(databaseName,
        tableName, true, Long.MAX_VALUE);

    OMTableSetPropertyRequest omTableSetPropertyRequest =
        new OMTableSetPropertyRequest(omRequest);

    OMClientResponse omClientResponse =
        omTableSetPropertyRequest.validateAndUpdateCache(ozoneManager, 1,
            ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.TABLE_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());

    Assert.assertNull(omMetadataManager.getMetaTable().get(
        omMetadataManager.getMetaTableKey(databaseName, tableName)));

  }

  private OMRequest createSetTablePropertyRequest(String databaseName,
      String tableName, boolean isVersionEnabled, long usedBytes) {

    return OMRequest.newBuilder().setSetTablePropertyRequest(
        SetTablePropertyRequest.newBuilder().setTableArgs(
            TableArgs.newBuilder().setTableName(tableName)
                .setDatabaseName(databaseName)
                .setUsedBytes(usedBytes)
                .setStorageType(StorageType.SSD.toProto())
                .setNumReplicas(3)
                .setSchema(getSchema().toProtobuf())
                .setBuckets(8)
                .setIsVersionEnabled(isVersionEnabled).build()))
        .setCmdType(OzoneManagerProtocolProtos.Type.SetTableProperty)
        .setClientId(UUID.randomUUID().toString()).build();
  }

  @Test
  public void testValidateAndUpdateCacheWithQuota() throws Exception {

    String databaseName = UUID.randomUUID().toString();
    String tableName = UUID.randomUUID().toString();

    TestOMRequestUtils.addDatabaseToDB(
        databaseName, omMetadataManager, 10 * GB);
    TestOMRequestUtils.addMetaTableToDB(
        databaseName, tableName, omMetadataManager, 8 * GB);
    OMRequest omRequest = createSetTablePropertyRequest(databaseName,
        tableName, true, 20 * GB);

    OMTableSetPropertyRequest omTableSetPropertyRequest =
        new OMTableSetPropertyRequest(omRequest);
    int countException = 0;
    try {
      omTableSetPropertyRequest.validateAndUpdateCache(ozoneManager, 1,
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
