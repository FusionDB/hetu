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

package org.apache.hadoop.hetu.om.request.database;

import org.apache.hadoop.hetu.om.request.TestOMRequestUtils;
import org.apache.hadoop.hetu.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Tests set database property request.
 */
public class TestOMDatabaseSetOwnerRequest extends TestOMDatabaseRequest {

  @Test
  public void testPreExecute() throws Exception {
    String databaseName = UUID.randomUUID().toString();
    String newOwner = "user1";
    OMRequest originalRequest =
        TestOMRequestUtils.createSetDatabasePropertyRequest(databaseName, newOwner);

    OMDatabaseSetQuotaRequest omDatabaseSetQuotaRequest =
        new OMDatabaseSetQuotaRequest(originalRequest);

    OMRequest modifiedRequest = omDatabaseSetQuotaRequest.preExecute(
        ozoneManager);
    Assert.assertNotEquals(modifiedRequest, originalRequest);
  }


  @Test
  public void testValidateAndUpdateCacheSuccess() throws Exception {
    String databaseName = UUID.randomUUID().toString();
    String ownerName = "user1";

    TestOMRequestUtils.addDbUserToDB(databaseName, ownerName, omMetadataManager);
    TestOMRequestUtils.addDatabaseToDB(databaseName, ownerName, omMetadataManager);

    String newOwner = "user2";

    OMRequest originalRequest =
        TestOMRequestUtils.createSetDatabasePropertyRequest(databaseName, newOwner);

    OMDatabaseSetOwnerRequest omDatabaseSetOwnerRequest =
        new OMDatabaseSetOwnerRequest(originalRequest);

    omDatabaseSetOwnerRequest.preExecute(ozoneManager);

    String databaseKey = omMetadataManager.getDatabaseKey(databaseName);
    String ownerKey = omMetadataManager.getUserKey(ownerName);
    String newOwnerKey = omMetadataManager.getUserKey(newOwner);



    OMClientResponse omClientResponse =
        omDatabaseSetOwnerRequest.validateAndUpdateCache(ozoneManager, 1,
            ozoneManagerDoubleBufferHelper);

    OzoneManagerProtocolProtos.OMResponse omResponse =
        omClientResponse.getOMResponse();
    Assert.assertNotNull(omResponse.getSetDatabasePropertyResponse());
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omResponse.getStatus());


    String fromDBOwner = omMetadataManager
        .getDatabaseTable().get(databaseKey).getOwnerName();
    Assert.assertEquals(newOwner, fromDBOwner);

    // modificationTime should be greater than creationTime.
    long creationTime = omMetadataManager.getDatabaseTable()
        .get(databaseKey).getCreationTime();
    long modificationTime = omMetadataManager.getDatabaseTable()
        .get(databaseKey).getModificationTime();
    Assert.assertTrue(modificationTime > creationTime);

    OzoneManagerStorageProtos.PersistedUserDatabaseInfo newOwnerDatabaseList =
        omMetadataManager.getUserTableDb().get(newOwnerKey);

    Assert.assertNotNull(newOwnerDatabaseList);
    Assert.assertEquals(databaseName,
        newOwnerDatabaseList.getDatabaseNamesList().get(0));

    OzoneManagerStorageProtos.PersistedUserDatabaseInfo oldOwnerDatabaseList =
        omMetadataManager.getUserTableDb().get(
            omMetadataManager.getUserKey(ownerKey));

    Assert.assertNotNull(oldOwnerDatabaseList);
    Assert.assertTrue(oldOwnerDatabaseList.getDatabaseNamesList().size() == 0);

  }


  @Test
  public void testValidateAndUpdateCacheWithDatabaseNotFound()
      throws Exception {
    String databaseName = UUID.randomUUID().toString();
    String ownerName = "user1";

    OMRequest originalRequest =
        TestOMRequestUtils.createSetDatabasePropertyRequest(databaseName,
            ownerName);

    OMDatabaseSetOwnerRequest omDatabaseSetOwnerRequest =
        new OMDatabaseSetOwnerRequest(originalRequest);

    omDatabaseSetOwnerRequest.preExecute(ozoneManager);

    OMClientResponse omClientResponse =
        omDatabaseSetOwnerRequest.validateAndUpdateCache(ozoneManager, 1,
            ozoneManagerDoubleBufferHelper);

    OzoneManagerProtocolProtos.OMResponse omResponse =
        omClientResponse.getOMResponse();
    Assert.assertNotNull(omResponse.getCreateDatabaseResponse());
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.DATABASE_NOT_FOUND,
        omResponse.getStatus());

  }

  @Test
  public void testInvalidRequest() throws Exception {
    String databaseName = UUID.randomUUID().toString();

    // create request with quota set.
    OMRequest originalRequest =
        TestOMRequestUtils.createSetDatabasePropertyRequest(databaseName,
            100L, 100L);

    OMDatabaseRequest omDatabaseSetOwnerRequest =
        new OMDatabaseSetOwnerRequest(originalRequest);

    omDatabaseSetOwnerRequest.preExecute(ozoneManager);

    OMClientResponse omClientResponse =
        omDatabaseSetOwnerRequest.validateAndUpdateCache(ozoneManager, 1,
            ozoneManagerDoubleBufferHelper);

    OzoneManagerProtocolProtos.OMResponse omResponse =
        omClientResponse.getOMResponse();
    Assert.assertNotNull(omResponse.getCreateDatabaseResponse());
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.INVALID_REQUEST,
        omResponse.getStatus());
  }


  @Test
  public void testOwnSameDatabaseTwice() throws Exception {
    String databaseName = UUID.randomUUID().toString();
    String owner = "user1";
    TestOMRequestUtils.addDatabaseToDB(databaseName, owner, omMetadataManager);
    TestOMRequestUtils.addDbUserToDB(databaseName, owner, omMetadataManager);
    String newOwner = "user2";

    // Create request to set new owner
    OMRequest omRequest =
        TestOMRequestUtils.createSetDatabasePropertyRequest(databaseName, newOwner);

    OMDatabaseSetOwnerRequest setOwnerRequest =
        new OMDatabaseSetOwnerRequest(omRequest);
    // Execute the request
    setOwnerRequest.preExecute(ozoneManager);
    OMClientResponse omClientResponse = setOwnerRequest.validateAndUpdateCache(
        ozoneManager, 1, ozoneManagerDoubleBufferHelper);
    // Response status should be OK and success flag should be true.
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());
    Assert.assertTrue(omClientResponse.getOMResponse().getSuccess());

    // Execute the same request again but with higher index
    setOwnerRequest.preExecute(ozoneManager);
    omClientResponse = setOwnerRequest.validateAndUpdateCache(
        ozoneManager, 2, ozoneManagerDoubleBufferHelper);
    // Response status should be OK, but success flag should be false.
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());
    Assert.assertFalse(omClientResponse.getOMResponse().getSuccess());

    // Check database names list
    OzoneManagerStorageProtos.PersistedUserDatabaseInfo userDatabaseInfo =
        omMetadataManager.getUserTableDb().get(newOwner);
    Assert.assertNotNull(userDatabaseInfo);
    List<String> databaseNamesList = userDatabaseInfo.getDatabaseNamesList();
    Assert.assertEquals(1, databaseNamesList.size());

    Set<String> databaseNamesSet = new HashSet<>(databaseNamesList);
    // If the set size isn't equal to list size, there are duplicates
    // in the list (which was the bug before the fix).
    Assert.assertEquals(databaseNamesList.size(), databaseNamesSet.size());
  }
}
