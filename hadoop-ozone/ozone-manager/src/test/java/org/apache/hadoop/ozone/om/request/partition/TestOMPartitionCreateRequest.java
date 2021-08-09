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

import org.apache.hadoop.ozone.hm.OmDatabaseArgs;
import org.apache.hadoop.ozone.hm.meta.table.ColumnKey;
import org.apache.hadoop.ozone.hm.meta.table.ColumnSchema;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmPartitionInfo;
import org.apache.hadoop.ozone.om.helpers.OmTableInfo;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.StorageTypeProto;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.util.Time;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.UUID;

/**
 * Tests OMPartitionCreateRequest class, which handles PartitionTable request.
 */
public class TestOMPartitionCreateRequest extends TestPartitionRequest {

  @Test
  public void testPreExecute() throws Exception {
    String databaseName = UUID.randomUUID().toString();
    String tableName = UUID.randomUUID().toString();
    String partitionName = UUID.randomUUID().toString();
    doPreExecute(databaseName, tableName, partitionName);
    // Verify invalid partition name throws exception
    LambdaTestUtils.intercept(OMException.class, "Invalid partition name: p1",
        () -> doPreExecute("db1", "t1", "p1"));
  }


  @Test
  public void testValidateAndUpdateCache() throws Exception {
    String databaseName = UUID.randomUUID().toString();
    String tableName = UUID.randomUUID().toString();
    String partitionName = UUID.randomUUID().toString();

    OMPartitionCreateRequest omPartitionCreateRequest = doPreExecute(databaseName,
        tableName, partitionName);

    doValidateAndUpdateCache(databaseName, tableName, partitionName,
            omPartitionCreateRequest.getOmRequest());

  }

  @Test
  public void testValidateAndUpdateCacheWithNoTable() throws Exception {
    String databaseName = UUID.randomUUID().toString();
    String tableName = UUID.randomUUID().toString();
    String partitionName = UUID.randomUUID().toString();

    // create database
    addCreateDatabaseToTable(databaseName, omMetadataManager);

    OMRequest originalRequest = TestOMRequestUtils.createPartitionRequest(
        tableName, databaseName, partitionName, false, StorageTypeProto.SSD);

    OMPartitionCreateRequest omPartitionCreateRequest =
        new OMPartitionCreateRequest(originalRequest);

    String partitionKey = omMetadataManager.getPartitionKey(databaseName, tableName, partitionName);

    // As we have not still called validateAndUpdateCache, get() should
    // return null.

    Assert.assertNull(omMetadataManager.getPartitionTable().get(partitionKey));

    OMClientResponse omClientResponse =
        omPartitionCreateRequest.validateAndUpdateCache(ozoneManager, 1,
            ozoneManagerDoubleBufferHelper);

    OMResponse omResponse = omClientResponse.getOMResponse();
    Assert.assertNotNull(omResponse.getCreatePartitionResponse());
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.TABLE_NOT_FOUND,
        omResponse.getStatus());

    // As request is invalid table partition should not have entry.
    Assert.assertNull(omMetadataManager.getPartitionTable().get(partitionKey));
  }


  @Test
  public void testValidateAndUpdateCacheWithPartitionAlreadyExists()
      throws Exception {
    String databaseName = UUID.randomUUID().toString();
    String tableName = UUID.randomUUID().toString();
    String partitionName = UUID.randomUUID().toString();

    OMPartitionCreateRequest omPartitionCreateRequest =
        doPreExecute(databaseName, tableName, partitionName);

    doValidateAndUpdateCache(databaseName, tableName, partitionName,
            omPartitionCreateRequest.getOmRequest());

    // Try create same partition again
    OMClientResponse omClientResponse =
        omPartitionCreateRequest.validateAndUpdateCache(ozoneManager, 2,
            ozoneManagerDoubleBufferHelper);

    OMResponse omResponse = omClientResponse.getOMResponse();
    Assert.assertNotNull(omResponse.getCreatePartitionResponse());
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.PARTITION_ALREADY_EXISTS,
        omResponse.getStatus());
  }


  private OMPartitionCreateRequest doPreExecute(String databaseName,
      String tableName, String partitionName) throws Exception {
    addCreateDatabaseToTable(databaseName, omMetadataManager);
    addCreateMetaTableToTable(databaseName, tableName, omMetadataManager);

    OMRequest originalRequest =
            TestOMRequestUtils.createPartitionRequest(tableName, databaseName, partitionName, false,
                    StorageTypeProto.SSD);

    OMPartitionCreateRequest omPartitionCreateRequest =
        new OMPartitionCreateRequest(originalRequest);

    OMRequest modifiedRequest = omPartitionCreateRequest.preExecute(ozoneManager);
    verifyRequest(modifiedRequest, originalRequest);
    return new OMPartitionCreateRequest(modifiedRequest);
  }

  private void doValidateAndUpdateCache(String databaseName, String tableName, String partitionName,
      OMRequest modifiedRequest) throws Exception {
    String partitionKey = omMetadataManager.getPartitionKey(databaseName, tableName, partitionName);

    // As we have not still called validateAndUpdateCache, get() should
    // return null.

    Assert.assertNull(omMetadataManager.getPartitionTable().get(partitionKey));
    OMPartitionCreateRequest omPartitionCreateRequest =
        new OMPartitionCreateRequest(modifiedRequest);


    OMClientResponse omClientResponse =
            omPartitionCreateRequest.validateAndUpdateCache(ozoneManager, 1,
            ozoneManagerDoubleBufferHelper);

    // As now after validateAndUpdateCache it should add entry to cache, get
    // should return non null value.
    OmPartitionInfo dbPartitionInfo =
        omMetadataManager.getPartitionTable().get(partitionKey);
    Assert.assertNotNull(dbPartitionInfo);

    // verify partition data with actual request data.
    OmPartitionInfo partitionInfoFromProto = OmPartitionInfo.getFromProtobuf(
        modifiedRequest.getCreatePartitionRequest().getPartitionInfo());

    Assert.assertEquals(partitionInfoFromProto.getCreationTime(),
        dbPartitionInfo.getCreationTime());
    Assert.assertEquals(partitionInfoFromProto.getPartitionName(),
            dbPartitionInfo.getPartitionName());
    Assert.assertEquals(partitionInfoFromProto.getModificationTime(),
            dbPartitionInfo.getModificationTime());
//    Assert.assertEquals(partitionInfoFromProto.getAcls(),
//        dbPartitionInfo.getAcls());
    Assert.assertEquals(partitionInfoFromProto.getIsVersionEnabled(),
            dbPartitionInfo.getIsVersionEnabled());
    Assert.assertEquals(partitionInfoFromProto.getStorageType(),
            dbPartitionInfo.getStorageType());
    Assert.assertEquals(partitionInfoFromProto.getMetadata(),
            dbPartitionInfo.getMetadata());

    // verify OMResponse.
    verifySuccessCreatePartitionResponse(omClientResponse.getOMResponse());

  }

  private void verifyRequest(OMRequest modifiedOmRequest,
      OMRequest originalRequest) {
    OzoneManagerProtocolProtos.PartitionInfo original =
        originalRequest.getCreatePartitionRequest().getPartitionInfo();
    OzoneManagerProtocolProtos.PartitionInfo updated =
        modifiedOmRequest.getCreatePartitionRequest().getPartitionInfo();

    Assert.assertEquals(original.getPartitionName(), updated.getPartitionName());
    Assert.assertEquals(original.getTableName(), updated.getTableName());
    Assert.assertEquals(original.getDatabaseName(), updated.getDatabaseName());
    Assert.assertEquals(original.getIsVersionEnabled(),
        updated.getIsVersionEnabled());
    Assert.assertEquals(original.getStorageType(), updated.getStorageType());
    Assert.assertEquals(original.getMetadataList(), updated.getMetadataList());
    Assert.assertNotEquals(original.getCreationTime(),
        updated.getCreationTime());
  }

  public static void verifySuccessCreatePartitionResponse(OMResponse omResponse) {
    Assert.assertNotNull(omResponse.getCreatePartitionResponse());
    Assert.assertEquals(OzoneManagerProtocolProtos.Type.CreatePartition,
        omResponse.getCmdType());
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omResponse.getStatus());
  }

  public static void addCreateDatabaseToTable(String databaseName,
      OMMetadataManager omMetadataManager) throws Exception {
    OmDatabaseArgs omDatabaseArgs =
        OmDatabaseArgs.newBuilder().setCreationTime(Time.now())
            .setName(databaseName).setAdminName(UUID.randomUUID().toString())
            .setOwnerName(UUID.randomUUID().toString()).build();
    TestOMRequestUtils.addDatabaseToOM(omMetadataManager, omDatabaseArgs);
  }

  public static void addCreateMetaTableToTable(String databaseName, String tableName,
                                              OMMetadataManager omMetadataManager) throws Exception {
    OmTableInfo omTableInfo = getOmTableInfo(databaseName, tableName);
    TestOMRequestUtils.addMetaTableToOM(omMetadataManager, omTableInfo);
  }

  private static OmTableInfo getOmTableInfo(String databaseName, String tableName) {
    ColumnSchema col1 = new ColumnSchema(
            "city",
            "varchar(4)",
            "varcher",
            0,
            "",
            "",
            "城市",
            true);

    ColumnSchema col2 = new ColumnSchema(
            "id",
            "Long",
            "Long",
            1,
            "",
            "",
            "ID",
            true);

    OzoneManagerProtocolProtos.TableInfo.PartitionsProto partitionsProto = getPartitionsProto();

    return OmTableInfo.newBuilder()
            .setDatabaseName(databaseName)
            .setTableName(tableName)
            .setColumns(Arrays.asList(col1, col2))
            .setPartitions(partitionsProto)
            .setDistributedKey(getDistributedKeyProto())
            .setColumnKey(getColumnKeyProto())
            .build();
  }

  @NotNull
  public static OzoneManagerProtocolProtos.TableInfo.PartitionsProto getPartitionsProto() {
    return OzoneManagerProtocolProtos.TableInfo.PartitionsProto.newBuilder()
            .addAllFields(Arrays.asList("city"))
            .setPartitionType(OzoneManagerProtocolProtos.TableInfo.Type.HASH)
            .build();
  }

  private static OzoneManagerProtocolProtos.TableInfo.DistributedKeyProto getDistributedKeyProto() {
    return OzoneManagerProtocolProtos.TableInfo.DistributedKeyProto
            .newBuilder()
            .setDistributedKeyType(OzoneManagerProtocolProtos.TableInfo.Type.HASH)
            .setBuckets(8)
            .addAllFields(Arrays.asList("id"))
            .build();
  }

  private static ColumnKey getColumnKeyProto() {
    OzoneManagerProtocolProtos.TableInfo.ColumnKeyProto columnKeyProto = OzoneManagerProtocolProtos.TableInfo.ColumnKeyProto
            .newBuilder()
            .setColumnKeyType(OzoneManagerProtocolProtos.TableInfo.ColumnKeyTypeProto.PRIMARY_KEY)
            .addAllFields(Arrays.asList("id"))
            .build();
    return ColumnKey.fromProtobuf(columnKeyProto);
  }
}
