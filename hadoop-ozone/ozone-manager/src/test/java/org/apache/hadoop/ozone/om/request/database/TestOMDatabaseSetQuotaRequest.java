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

import org.apache.hadoop.ozone.hm.HmDatabaseArgs;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.om.request.volume.OMVolumeSetQuotaRequest;
import org.apache.hadoop.ozone.om.request.volume.TestOMVolumeRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

import static org.apache.hadoop.ozone.OzoneConsts.GB;

/**
 * Tests set database property request.
 */
public class TestOMDatabaseSetQuotaRequest extends TestOMDatabaseRequest {

  @Test
  public void testPreExecute() throws Exception {
    String databaseName = UUID.randomUUID().toString();
    long quotaInBytes = 100L;
    long quotaInNamespace = 1000L;
    OMRequest originalRequest =
        TestOMRequestUtils.createSetDatabasePropertyRequest(databaseName,
            quotaInBytes, quotaInNamespace);

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
    long quotaInBytes = 100L;
    long quotaInNamespace = 1000L;

    TestOMRequestUtils.addDbUserToDB(databaseName, ownerName, omMetadataManager);
    TestOMRequestUtils.addDatabaseToDB(databaseName, ownerName, omMetadataManager);

    OMRequest originalRequest =
        TestOMRequestUtils.createSetDatabasePropertyRequest(databaseName,
            quotaInBytes, quotaInNamespace);

    OMDatabaseSetQuotaRequest omDatabaseSetQuotaRequest =
        new OMDatabaseSetQuotaRequest(originalRequest);

    omDatabaseSetQuotaRequest.preExecute(ozoneManager);

    String databaseKey = omMetadataManager.getDatabaseKey(databaseName);

    // Get Quota before validateAndUpdateCache.
    HmDatabaseArgs hmDatabaseArgs =
        omMetadataManager.getDatabaseTable().get(databaseKey);
    // As request is valid database table should not have entry.
    Assert.assertNotNull(hmDatabaseArgs);
    long quotaBytesBeforeSet = hmDatabaseArgs.getQuotaInBytes();
    long quotaNamespaceBeforeSet = hmDatabaseArgs.getQuotaInNamespace();

    OMClientResponse omClientResponse =
        omDatabaseSetQuotaRequest.validateAndUpdateCache(ozoneManager, 1,
            ozoneManagerDoubleBufferHelper);

    OzoneManagerProtocolProtos.OMResponse omResponse =
        omClientResponse.getOMResponse();
    Assert.assertNotNull(omResponse.getSetDatabasePropertyResponse());
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omResponse.getStatus());


    HmDatabaseArgs ova = omMetadataManager.getDatabaseTable().get(databaseKey);
    long quotaBytesAfterSet = ova.getQuotaInBytes();
    long quotaNamespaceAfterSet = ova.getQuotaInNamespace();
    Assert.assertEquals(quotaInBytes, quotaBytesAfterSet);
    Assert.assertEquals(quotaInNamespace, quotaNamespaceAfterSet);
    Assert.assertNotEquals(quotaBytesBeforeSet, quotaBytesAfterSet);
    Assert.assertNotEquals(quotaNamespaceBeforeSet, quotaNamespaceAfterSet);

    // modificationTime should be greater than creationTime.
    long creationTime = omMetadataManager
        .getDatabaseTable().get(databaseKey).getCreationTime();
    long modificationTime = omMetadataManager
        .getDatabaseTable().get(databaseKey).getModificationTime();
    Assert.assertTrue(modificationTime > creationTime);
  }

  @Test
  public void testValidateAndUpdateCacheWithDatabaseNotFound()
      throws Exception {
    String databaseName = UUID.randomUUID().toString();
    long quotaInBytes = 100L;
    long quotaInNamespace= 100L;

    OMRequest originalRequest =
        TestOMRequestUtils.createSetDatabasePropertyRequest(databaseName,
            quotaInBytes, quotaInNamespace);

    OMDatabaseSetQuotaRequest omDatabaseSetQuotaRequest =
        new OMDatabaseSetQuotaRequest(originalRequest);

    omDatabaseSetQuotaRequest.preExecute(ozoneManager);

    OMClientResponse omClientResponse =
        omDatabaseSetQuotaRequest.validateAndUpdateCache(ozoneManager, 1,
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

    // create request with owner set.
    OMRequest originalRequest =
        TestOMRequestUtils.createSetDatabasePropertyRequest(databaseName,
            "user1");

    // Creating OMDatabaseSetQuotaRequest with SetProperty request set with owner.
    OMDatabaseSetQuotaRequest omDatabaseSetQuotaRequest =
        new OMDatabaseSetQuotaRequest(originalRequest);

    omDatabaseSetQuotaRequest.preExecute(ozoneManager);
    OMClientResponse omClientResponse =
        omDatabaseSetQuotaRequest.validateAndUpdateCache(ozoneManager, 1,
            ozoneManagerDoubleBufferHelper);

    OzoneManagerProtocolProtos.OMResponse omResponse =
        omClientResponse.getOMResponse();
    Assert.assertNotNull(omResponse.getCreateDatabaseResponse());
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.INVALID_REQUEST,
        omResponse.getStatus());
  }

  @Test
  public void testValidateAndUpdateCacheWithQuota() throws Exception {
    String databaseName = UUID.randomUUID().toString();
    String tableName = UUID.randomUUID().toString();

    TestOMRequestUtils.addDatabaseToDB(
        databaseName, omMetadataManager, 10 * GB);
    TestOMRequestUtils.addMetaTableToDB(
        databaseName, tableName, omMetadataManager, 8 * GB);
    OMRequest originalRequest =
        TestOMRequestUtils.createSetDatabasePropertyRequest(databaseName,
            5 * GB, 100L);

    OMDatabaseSetQuotaRequest omDatabaseSetQuotaRequest =
        new OMDatabaseSetQuotaRequest(originalRequest);

    int countException = 0;
    try {
      omDatabaseSetQuotaRequest.validateAndUpdateCache(ozoneManager, 1,
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
