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

import org.apache.hadoop.hetu.om.request.TestOMRequestUtils;
import org.apache.hadoop.hetu.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteTableRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

/**
 * Tests OMTableDeleteRequest class which handles DeleteTable request.
 */
public class TestOMTableDeleteRequest extends TestTableRequest {

  @Test
  public void testPreExecute() throws Exception {
    OMRequest omRequest =
        createDeleteTableRequest(UUID.randomUUID().toString(),
            UUID.randomUUID().toString());

    OMTableDeleteRequest omTableDeleteRequest =
        new OMTableDeleteRequest(omRequest);

    // As user info gets added.
    Assert.assertNotEquals(omRequest,
        omTableDeleteRequest.preExecute(ozoneManager));
  }

  @Test
  public void testValidateAndUpdateCache() throws Exception {
    String databaseName = UUID.randomUUID().toString();
    String tableName = UUID.randomUUID().toString();
    OMRequest omRequest =
        createDeleteTableRequest(databaseName, tableName);

    OMTableDeleteRequest omTableDeleteRequest =
        new OMTableDeleteRequest(omRequest);

    // Create database and table entries in DB.
    TestOMRequestUtils.addDatabaseAndTableToDB(databaseName, tableName,
        omMetadataManager);

    omTableDeleteRequest.validateAndUpdateCache(ozoneManager, 1,
        ozoneManagerDoubleBufferHelper);

    Assert.assertNull(omMetadataManager.getMetaTable().get(
        omMetadataManager.getMetaTableKey(databaseName, tableName)));
  }

  @Test
  public void testValidateAndUpdateCacheFailure() throws Exception {
    String databaseName = UUID.randomUUID().toString();
    String tableName = UUID.randomUUID().toString();

    OMRequest omRequest =
        createDeleteTableRequest(databaseName, tableName);

    OMTableDeleteRequest omTableDeleteRequest =
        new OMTableDeleteRequest(omRequest);


    OMClientResponse omClientResponse =
        omTableDeleteRequest.validateAndUpdateCache(ozoneManager, 1,
            ozoneManagerDoubleBufferHelper);

    Assert.assertNull(omMetadataManager.getMetaTable().get(
        omMetadataManager.getMetaTableKey(databaseName, tableName)));

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.TABLE_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());

    TestOMRequestUtils.addDatabaseAndTableToDB(databaseName, tableName,
        omMetadataManager);
  }

  private OMRequest createDeleteTableRequest(String databaseName,
      String tableName) {
    return OMRequest.newBuilder().setDeleteTableRequest(
        DeleteTableRequest.newBuilder()
            .setTableName(tableName).setDatabaseName(databaseName))
        .setCmdType(OzoneManagerProtocolProtos.Type.DeleteTable)
        .setClientId(UUID.randomUUID().toString()).build();
  }
}
