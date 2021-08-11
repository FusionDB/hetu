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

import org.apache.hadoop.hetu.hm.helpers.OmDatabaseArgs;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmTableInfo;
import org.apache.hadoop.hetu.om.request.TestOMRequestUtils;
import org.apache.hadoop.hetu.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.StorageTypeProto;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.util.Time;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

/**
 * Tests OMTableCreateRequest class, which handles CreateTable request.
 */
public class TestOMTableCreateRequest extends TestTableRequest {

  @Test
  public void testPreExecute() throws Exception {
    String databaseName = UUID.randomUUID().toString();
    String tableName = UUID.randomUUID().toString();
    doPreExecute(databaseName, tableName);
    // Verify invalid table name throws exception
    LambdaTestUtils.intercept(OMException.class, "Invalid table name: t1",
        () -> doPreExecute("db1", "t1"));
  }


  @Test
  public void testValidateAndUpdateCache() throws Exception {
    String databaseName = UUID.randomUUID().toString();
    String tableName = UUID.randomUUID().toString();

    OMTableCreateRequest omTableCreateRequest = doPreExecute(databaseName,
        tableName);

    doValidateAndUpdateCache(databaseName, tableName,
        omTableCreateRequest.getOmRequest());

  }

  @Test
  public void testValidateAndUpdateCacheWithNoDatabase() throws Exception {
    String databaseName = UUID.randomUUID().toString();
    String tableName = UUID.randomUUID().toString();

    OMRequest originalRequest = TestOMRequestUtils.createTableRequest(
        tableName, databaseName, false, StorageTypeProto.SSD);

    OMTableCreateRequest omTableCreateRequest =
        new OMTableCreateRequest(originalRequest);

    String tableKey = omMetadataManager.getMetaTableKey(databaseName, tableName);

    // As we have not still called validateAndUpdateCache, get() should
    // return null.

    Assert.assertNull(omMetadataManager.getMetaTable().get(tableKey));

    OMClientResponse omClientResponse =
        omTableCreateRequest.validateAndUpdateCache(ozoneManager, 1,
            ozoneManagerDoubleBufferHelper);

    OMResponse omResponse = omClientResponse.getOMResponse();
    Assert.assertNotNull(omResponse.getCreateTableResponse());
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.DATABASE_NOT_FOUND,
        omResponse.getStatus());

    // As request is invalid table table should not have entry.
    Assert.assertNull(omMetadataManager.getMetaTable().get(tableKey));
  }


  @Test
  public void testValidateAndUpdateCacheWithTableAlreadyExists()
      throws Exception {
    String databaseName = UUID.randomUUID().toString();
    String tableName = UUID.randomUUID().toString();

    OMTableCreateRequest omTableCreateRequest =
        doPreExecute(databaseName, tableName);

    doValidateAndUpdateCache(databaseName, tableName,
        omTableCreateRequest.getOmRequest());

    // Try create same table again
    OMClientResponse omClientResponse =
        omTableCreateRequest.validateAndUpdateCache(ozoneManager, 2,
            ozoneManagerDoubleBufferHelper);

    OMResponse omResponse = omClientResponse.getOMResponse();
    Assert.assertNotNull(omResponse.getCreateTableResponse());
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.TABLE_ALREADY_EXISTS,
        omResponse.getStatus());
  }


  private OMTableCreateRequest doPreExecute(String databaseName,
      String tableName) throws Exception {
    addCreateDatabaseToTable(databaseName, omMetadataManager);
    OMRequest originalRequest =
        TestOMRequestUtils.createTableRequest(tableName, databaseName, false,
            StorageTypeProto.SSD);

    OMTableCreateRequest omTableCreateRequest =
        new OMTableCreateRequest(originalRequest);

    OMRequest modifiedRequest = omTableCreateRequest.preExecute(ozoneManager);
    verifyRequest(modifiedRequest, originalRequest);
    return new OMTableCreateRequest(modifiedRequest);
  }

  private void doValidateAndUpdateCache(String databaseName, String tableName,
      OMRequest modifiedRequest) throws Exception {
    String tableKey = omMetadataManager.getMetaTableKey(databaseName, tableName);

    // As we have not still called validateAndUpdateCache, get() should
    // return null.

    Assert.assertNull(omMetadataManager.getMetaTable().get(tableKey));
    OMTableCreateRequest omTableCreateRequest =
        new OMTableCreateRequest(modifiedRequest);


    OMClientResponse omClientResponse =
            omTableCreateRequest.validateAndUpdateCache(ozoneManager, 1,
            ozoneManagerDoubleBufferHelper);

    // As now after validateAndUpdateCache it should add entry to cache, get
    // should return non null value.
    OmTableInfo dbTableInfo =
        omMetadataManager.getMetaTable().get(tableKey);
    Assert.assertNotNull(omMetadataManager.getMetaTable().get(tableKey));

    // verify table data with actual request data.
    OmTableInfo tableInfoFromProto = OmTableInfo.getFromProtobuf(
        modifiedRequest.getCreateTableRequest().getTableInfo());

    Assert.assertEquals(tableInfoFromProto.getCreationTime(),
        dbTableInfo.getCreationTime());
    Assert.assertEquals(tableInfoFromProto.getModificationTime(),
        dbTableInfo.getModificationTime());
//    Assert.assertEquals(tableInfoFromProto.getAcls(),
//        dbTableInfo.getAcls());
    Assert.assertEquals(tableInfoFromProto.getIsVersionEnabled(),
        dbTableInfo.getIsVersionEnabled());
    Assert.assertEquals(tableInfoFromProto.getStorageType(),
        dbTableInfo.getStorageType());
    Assert.assertEquals(tableInfoFromProto.getMetadata(),
        dbTableInfo.getMetadata());
//    Assert.assertEquals(tableInfoFromProto.getEncryptionKeyInfo(),
//        dbTableInfo.getEncryptionKeyInfo());

    // verify OMResponse.
    verifySuccessCreateTableResponse(omClientResponse.getOMResponse());

  }

  private void verifyRequest(OMRequest modifiedOmRequest,
      OMRequest originalRequest) {
    OzoneManagerProtocolProtos.TableInfo original =
        originalRequest.getCreateTableRequest().getTableInfo();
    OzoneManagerProtocolProtos.TableInfo updated =
        modifiedOmRequest.getCreateTableRequest().getTableInfo();

    Assert.assertEquals(original.getTableName(), updated.getTableName());
    Assert.assertEquals(original.getDatabaseName(), updated.getDatabaseName());
    Assert.assertEquals(original.getIsVersionEnabled(),
        updated.getIsVersionEnabled());
    Assert.assertEquals(original.getStorageType(), updated.getStorageType());
    Assert.assertEquals(original.getMetadataList(), updated.getMetadataList());
    Assert.assertNotEquals(original.getCreationTime(),
        updated.getCreationTime());
  }

  public static void verifySuccessCreateTableResponse(OMResponse omResponse) {
    Assert.assertNotNull(omResponse.getCreateTableResponse());
    Assert.assertEquals(OzoneManagerProtocolProtos.Type.CreateTable,
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
}
