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

import org.apache.hadoop.hetu.hm.meta.table.ColumnSchema;
import org.apache.hadoop.ozone.om.helpers.OmTableInfo;
import org.apache.hadoop.hetu.om.request.TestOMRequestUtils;
import org.apache.hadoop.hetu.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteDatabaseRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.UUID;

import static org.apache.hadoop.hetu.om.request.TestOMRequestUtils.getSchema;


/**
 * Tests delete database request.
 */
public class TestOMDatabaseDeleteRequest extends TestOMDatabaseRequest {

  @Test
  public void testPreExecute() throws Exception {
    String databaseName = UUID.randomUUID().toString();
    OMRequest originalRequest = deleteDatabaseRequest(databaseName);

    OMDatabaseDeleteRequest omDatabaseDeleteRequest =
        new OMDatabaseDeleteRequest(originalRequest);

    OMRequest modifiedRequest = omDatabaseDeleteRequest.preExecute(ozoneManager);
    Assert.assertNotEquals(originalRequest, modifiedRequest);
  }

  @Test
  public void testValidateAndUpdateCacheSuccess() throws Exception {
    String databaseName = UUID.randomUUID().toString();
    String ownerName = "user1";

    OMRequest originalRequest = deleteDatabaseRequest(databaseName);

    OMDatabaseDeleteRequest omDatabaseDeleteRequest =
        new OMDatabaseDeleteRequest(originalRequest);

    omDatabaseDeleteRequest.preExecute(ozoneManager);

    // Add database and user to DB
    TestOMRequestUtils.addDatabaseToDB(databaseName, ownerName, omMetadataManager);
    // Put to userTableDb
    TestOMRequestUtils.addDbUserToDB(databaseName, ownerName, omMetadataManager);

    String databaseKey = omMetadataManager.getDatabaseKey(databaseName);
    String ownerKey = omMetadataManager.getUserKey(ownerName);


    Assert.assertNotNull(omMetadataManager.getDatabaseTable().get(databaseKey));
    Assert.assertNotNull(omMetadataManager.getUserTableDb().get(ownerKey));

    OMClientResponse omClientResponse =
        omDatabaseDeleteRequest.validateAndUpdateCache(ozoneManager, 1,
            ozoneManagerDoubleBufferHelper);

    OzoneManagerProtocolProtos.OMResponse omResponse =
        omClientResponse.getOMResponse();
    Assert.assertNotNull(omResponse.getCreateDatabaseResponse());
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omResponse.getStatus());

    Assert.assertTrue(omMetadataManager.getUserTableDb().get(ownerKey)
        .getDatabaseNamesList().size() == 0);
    // As now database is deleted, table should not have those entries.
    Assert.assertNull(omMetadataManager.getDatabaseTable().get(databaseKey));
  }

  @Test
  public void testValidateAndUpdateCacheWithDatabaseNotFound()
      throws Exception {
    String databaseName = UUID.randomUUID().toString();
    OMRequest originalRequest = deleteDatabaseRequest(databaseName);

    OMDatabaseDeleteRequest omDatabaseDeleteRequest =
        new OMDatabaseDeleteRequest(originalRequest);

    omDatabaseDeleteRequest.preExecute(ozoneManager);

    OMClientResponse omClientResponse =
        omDatabaseDeleteRequest.validateAndUpdateCache(ozoneManager, 1,
            ozoneManagerDoubleBufferHelper);

    OzoneManagerProtocolProtos.OMResponse omResponse =
        omClientResponse.getOMResponse();
    Assert.assertNotNull(omResponse.getCreateDatabaseResponse());
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.DATABASE_NOT_FOUND,
        omResponse.getStatus());
  }

  @Test
  public void testValidateAndUpdateCacheWithDatabaseNotEmpty() throws Exception {
    String databaseName = UUID.randomUUID().toString();
    String ownerName = "user1";

    OMRequest originalRequest = deleteDatabaseRequest(databaseName);

    OMDatabaseDeleteRequest omDatabaseDeleteRequest =
        new OMDatabaseDeleteRequest(originalRequest);

    omDatabaseDeleteRequest.preExecute(ozoneManager);

    // Add some table to table cache.
    String tableName = UUID.randomUUID().toString();

    OmTableInfo omTableInfo = OmTableInfo.newBuilder()
        .setDatabaseName(databaseName)
        .setTableName(tableName)
        .setSchema(getSchema())
        .setBuckets(8)
        .build();
    TestOMRequestUtils.addMetaTableToOM(omMetadataManager, omTableInfo);

    // Add user and database to DB.
    TestOMRequestUtils.addDbUserToDB(databaseName, ownerName, omMetadataManager);
    TestOMRequestUtils.addDatabaseToDB(databaseName, ownerName, omMetadataManager);

    OMClientResponse omClientResponse =
        omDatabaseDeleteRequest.validateAndUpdateCache(ozoneManager, 1L,
            ozoneManagerDoubleBufferHelper);

    OzoneManagerProtocolProtos.OMResponse omResponse =
        omClientResponse.getOMResponse();
    Assert.assertNotNull(omResponse.getCreateDatabaseResponse());
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.DATABASE_NOT_EMPTY,
        omResponse.getStatus());
  }

  /**
   * Create OMRequest for delete database.
   * @param databaseName
   * @return OMRequest
   */
  private OMRequest deleteDatabaseRequest(String databaseName) {
    DeleteDatabaseRequest deleteDatabaseRequest =
        DeleteDatabaseRequest.newBuilder().setName(databaseName).build();

    return OMRequest.newBuilder().setClientId(UUID.randomUUID().toString())
        .setCmdType(OzoneManagerProtocolProtos.Type.DeleteDatabase)
        .setDeleteDatabaseRequest(deleteDatabaseRequest).build();
  }
}
