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

package org.apache.hadoop.ozone.om.request.database;

import org.apache.hadoop.ozone.hm.OmDatabaseArgs;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.database.OMDatabaseCreateResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DatabaseInfo;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

import static org.mockito.Mockito.when;

/**
 * Tests create database request.
 */

public class TestOMDatabaseCreateRequest extends TestOMDatabaseRequest {

  @Test
  public void testPreExecute() throws Exception {
    String databaseName = UUID.randomUUID().toString();
    String adminName = UUID.randomUUID().toString();
    String ownerName = UUID.randomUUID().toString();
    doPreExecute(databaseName, adminName, ownerName);
    // Verify exception thrown on invalid database name
    LambdaTestUtils.intercept(OMException.class, "Invalid database name: v1",
        () -> doPreExecute("v1", adminName, ownerName));
  }

  @Test
  public void testValidateAndUpdateCacheWithZeroMaxUserDatabaseCount()
      throws Exception {
    when(ozoneManager.getMaxUserDatabaseCount()).thenReturn(0L);
    String databaseName = UUID.randomUUID().toString();
    String adminName = "user1";
    String ownerName = "user1";
    long txLogIndex = 1;
    long expectedObjId = ozoneManager.getObjectIdFromTxId(txLogIndex);

    OMRequest originalRequest = createDatabaseRequest(databaseName, adminName,
        ownerName);

    OMDatabaseCreateRequest omDatabaseCreateRequest =
        new OMDatabaseCreateRequest(originalRequest);

    omDatabaseCreateRequest.preExecute(ozoneManager);

    try {
      OMClientResponse omClientResponse =
           omDatabaseCreateRequest.validateAndUpdateCache(ozoneManager,
              txLogIndex, ozoneManagerDoubleBufferHelper);
      Assert.assertTrue(omClientResponse instanceof OMDatabaseCreateResponse);
      OMDatabaseCreateResponse respone =
          (OMDatabaseCreateResponse) omClientResponse;
      Assert.assertEquals(expectedObjId, respone.getOmDatabaseArgs()
          .getObjectID());
      Assert.assertEquals(txLogIndex, respone.getOmDatabaseArgs().getUpdateID());
    } catch (IllegalArgumentException ex){
      GenericTestUtils.assertExceptionContains("should be greater than zero",
          ex);
    }
  }

  @Test
  public void testValidateAndUpdateCacheSuccess() throws Exception {
    String databaseName = UUID.randomUUID().toString();
    String adminName = "user1";
    String ownerName = "user1";

    OMRequest originalRequest = createDatabaseRequest(databaseName, adminName,
        ownerName);

    OMDatabaseCreateRequest omDatabaseCreateRequest =
        new OMDatabaseCreateRequest(originalRequest);

    OMRequest modifiedRequest = omDatabaseCreateRequest.preExecute(ozoneManager);

    String databaseKey = omMetadataManager.getDatabaseKey(databaseName);
    String ownerKey = omMetadataManager.getUserKey(ownerName);

    // As we have not still called validateAndUpdateCache, get() should
    // return null.

    Assert.assertNull(omMetadataManager.getDatabaseTable().get(databaseKey));
    Assert.assertNull(omMetadataManager.getUserTable().get(ownerKey));

    omDatabaseCreateRequest = new OMDatabaseCreateRequest(modifiedRequest);
    long txLogIndex = 2;
    long expectedObjId = ozoneManager.getObjectIdFromTxId(txLogIndex);

    OMClientResponse omClientResponse =
        omDatabaseCreateRequest.validateAndUpdateCache(ozoneManager, txLogIndex,
            ozoneManagerDoubleBufferHelper);

    OzoneManagerProtocolProtos.OMResponse omResponse =
        omClientResponse.getOMResponse();
    Assert.assertNotNull(omResponse.getCreateDatabaseResponse());
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omResponse.getStatus());


    // Get databaseInfo from request.
    DatabaseInfo databaseInfo = omDatabaseCreateRequest.getOmRequest()
        .getCreateDatabaseRequest().getDatabaseInfo();

    OmDatabaseArgs omDatabaseArgs =
        omMetadataManager.getDatabaseTable().get(databaseKey);
    // As request is valid database table should not have entry.
    Assert.assertNotNull(omDatabaseArgs);
    Assert.assertEquals(expectedObjId, omDatabaseArgs.getObjectID());
    Assert.assertEquals(txLogIndex, omDatabaseArgs.getUpdateID());

    // Initial modificationTime should be equal to creationTime.
    long creationTime = omDatabaseArgs.getCreationTime();
    long modificationTime = omDatabaseArgs.getModificationTime();
    Assert.assertEquals(creationTime, modificationTime);

    // Check data from table and request.
    Assert.assertEquals(databaseInfo.getName(), omDatabaseArgs.getName());
    Assert.assertEquals(databaseInfo.getOwnerName(), omDatabaseArgs.getOwnerName());
    Assert.assertEquals(databaseInfo.getAdminName(), omDatabaseArgs.getAdminName());
    Assert.assertEquals(databaseInfo.getCreationTime(),
            omDatabaseArgs.getCreationTime());

    OzoneManagerStorageProtos.PersistedUserDatabaseInfo userDatabaseInfo =
        omMetadataManager.getUserTableDb().get(ownerKey);
    Assert.assertNotNull(userDatabaseInfo);
    Assert.assertEquals(databaseName, userDatabaseInfo.getDatabaseNames(0));

    // Create another database for the user.
    originalRequest = createDatabaseRequest("vol1", adminName,
        ownerName);

    omDatabaseCreateRequest =
        new OMDatabaseCreateRequest(originalRequest);

    modifiedRequest = omDatabaseCreateRequest.preExecute(ozoneManager);

    omClientResponse =
        omDatabaseCreateRequest.validateAndUpdateCache(ozoneManager, 2L,
            ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());

    Assert.assertTrue(omMetadataManager
        .getUserTableDb().get(ownerKey).getDatabaseNamesList().size() == 2);
  }

  @Test
  public void testValidateAndUpdateCacheWithDatabaseAlreadyExists()
      throws Exception {
    String databaseName = UUID.randomUUID().toString();
    String adminName = "user1";
    String ownerName = "user1";

    TestOMRequestUtils.addDatabaseToDB(databaseName, omMetadataManager);

    OMRequest originalRequest = createDatabaseRequest(databaseName, adminName,
        ownerName);

    OMDatabaseCreateRequest omDatabaseCreateRequest =
        new OMDatabaseCreateRequest(originalRequest);

    OMRequest modifiedRequest = omDatabaseCreateRequest.preExecute(ozoneManager);

    omDatabaseCreateRequest = new OMDatabaseCreateRequest(modifiedRequest);

    OMClientResponse omClientResponse =
        omDatabaseCreateRequest.validateAndUpdateCache(ozoneManager, 1,
            ozoneManagerDoubleBufferHelper);

    OzoneManagerProtocolProtos.OMResponse omResponse =
        omClientResponse.getOMResponse();
    Assert.assertNotNull(omResponse.getCreateDatabaseResponse());
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.DATABASE_ALREADY_EXISTS,
        omResponse.getStatus());

    // Check really if we have a database with the specified database name.
    Assert.assertNotNull(omMetadataManager.getDatabaseTable().get(
        omMetadataManager.getDatabaseKey(databaseName)));
  }

  private void doPreExecute(String databaseName,
      String adminName, String ownerName) throws Exception {

    OMRequest originalRequest = createDatabaseRequest(databaseName, adminName,
        ownerName);

    OMDatabaseCreateRequest omDatabaseCreateRequest =
        new OMDatabaseCreateRequest(originalRequest);

    OMRequest modifiedRequest = omDatabaseCreateRequest.preExecute(ozoneManager);
    verifyRequest(modifiedRequest, originalRequest);
  }

  /**
   * Verify modifiedOmRequest and originalRequest.
   * @param modifiedRequest
   * @param originalRequest
   */
  private void verifyRequest(OMRequest modifiedRequest,
      OMRequest originalRequest) {
    DatabaseInfo original = originalRequest.getCreateDatabaseRequest()
        .getDatabaseInfo();
    DatabaseInfo updated = modifiedRequest.getCreateDatabaseRequest()
        .getDatabaseInfo();

    Assert.assertEquals(original.getAdminName(), updated.getAdminName());
    Assert.assertEquals(original.getName(), updated.getName());
    Assert.assertEquals(original.getOwnerName(),
        updated.getOwnerName());
    Assert.assertNotEquals(original.getCreationTime(),
        updated.getCreationTime());
    Assert.assertNotEquals(original.getModificationTime(),
        updated.getModificationTime());
  }
}
