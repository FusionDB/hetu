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

package org.apache.hadoop.hetu.om.response.database;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.hm.OmDatabaseArgs;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.hetu.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateDatabaseResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos.PersistedUserDatabaseInfo;
import org.apache.hadoop.util.Time;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.UUID;

import static org.junit.Assert.fail;

/**
 * This class tests MDatabaseDeleteResponse.
 */
public class TestOMDatabaseDeleteResponse {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private OMMetadataManager omMetadataManager;
  private BatchOperation batchOperation;

  @Before
  public void setup() throws Exception {
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        folder.newFolder().getAbsolutePath());
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration);
    batchOperation = omMetadataManager.getStore().initBatchOperation();
  }

  @After
  public void tearDown() {
    if (batchOperation != null) {
      batchOperation.close();
    }
  }

  @Test
  public void testAddToDBBatch() throws Exception {

    String databaseName = UUID.randomUUID().toString();
    String userName = "user1";
    PersistedUserDatabaseInfo databaseList = PersistedUserDatabaseInfo.newBuilder()
        .setObjectID(1)
        .setUpdateID(1)
        .addDatabaseNames(databaseName).build();

    OMResponse omResponse = OMResponse.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.DeleteDatabase)
        .setStatus(OzoneManagerProtocolProtos.Status.OK)
        .setSuccess(true)
        .setCreateDatabaseResponse(CreateDatabaseResponse.getDefaultInstance())
        .build();

    OmDatabaseArgs omDatabaseArgs = OmDatabaseArgs.newBuilder()
        .setOwnerName(userName).setAdminName(userName)
        .setName(databaseName).setCreationTime(Time.now()).build();
    OMDatabaseCreateResponse omDatabaseCreateResponse =
        new OMDatabaseCreateResponse(omResponse, omDatabaseArgs, databaseList);

    // As we are deleting updated database list should be empty.
    PersistedUserDatabaseInfo updatedDatabaseList =
        PersistedUserDatabaseInfo.newBuilder()
        .setObjectID(1).setUpdateID(1).build();
    OMDatabaseDeleteResponse omDatabaseDeleteResponse =
        new OMDatabaseDeleteResponse(omResponse, databaseName, userName,
            updatedDatabaseList);

    omDatabaseCreateResponse.addToDBBatch(omMetadataManager, batchOperation);
    omDatabaseDeleteResponse.addToDBBatch(omMetadataManager, batchOperation);

    // Do manual commit and see whether addToBatch is successful or not.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    Assert.assertNull(null,
        omMetadataManager.getDatabaseTable().get(
            omMetadataManager.getDatabaseKey(databaseName)));

    Assert.assertEquals(null,
        omMetadataManager.getUserTableDb().get(
            omMetadataManager.getUserKey(userName)));
  }

  @Test
  public void testAddToDBBatchNoOp() throws Exception {

    OMResponse omResponse = OMResponse.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.DeleteDatabase)
        .setStatus(OzoneManagerProtocolProtos.Status.DATABASE_NOT_FOUND)
        .setSuccess(false)
        .setCreateDatabaseResponse(CreateDatabaseResponse.getDefaultInstance())
        .build();

    OMDatabaseDeleteResponse omDatabaseDeleteResponse = new OMDatabaseDeleteResponse(
        omResponse);

    try {
      omDatabaseDeleteResponse.checkAndUpdateDB(omMetadataManager,
          batchOperation);
    } catch (IOException ex) {
      fail("testAddToDBBatchFailure failed");
    }

  }


}
