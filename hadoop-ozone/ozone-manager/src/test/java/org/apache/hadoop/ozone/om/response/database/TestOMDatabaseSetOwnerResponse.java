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

package org.apache.hadoop.ozone.om.response.database;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.hm.HmDatabaseArgs;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
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
 * This class tests OMDatabaseCreateResponse.
 */
public class TestOMDatabaseSetOwnerResponse {

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
    String oldOwner = "user1";
    PersistedUserDatabaseInfo databaseList = PersistedUserDatabaseInfo.newBuilder()
        .setObjectID(1)
        .setUpdateID(1)
        .addDatabaseNames(databaseName).build();

    OMResponse omResponse = OMResponse.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.SetDatabaseProperty)
        .setStatus(OzoneManagerProtocolProtos.Status.OK)
        .setSuccess(true)
        .setCreateDatabaseResponse(CreateDatabaseResponse.getDefaultInstance())
        .build();

    HmDatabaseArgs hmDatabaseArgs = HmDatabaseArgs.newBuilder()
        .setOwnerName(oldOwner).setAdminName(oldOwner)
        .setName(databaseName).setCreationTime(Time.now()).build();
    OMDatabaseCreateResponse omDatabaseCreateResponse =
        new OMDatabaseCreateResponse(omResponse, hmDatabaseArgs, databaseList);



    String newOwner = "user2";
    PersistedUserDatabaseInfo newOwnerDatabaseList =
        PersistedUserDatabaseInfo.newBuilder()
        .setObjectID(1)
        .setUpdateID(1)
        .addDatabaseNames(databaseName).build();
    PersistedUserDatabaseInfo oldOwnerDatabaseList =
        PersistedUserDatabaseInfo.newBuilder()
        .setObjectID(2)
        .setUpdateID(2)
        .build();
    HmDatabaseArgs newOwnerDatabaseArgs = HmDatabaseArgs.newBuilder()
        .setOwnerName(newOwner).setAdminName(newOwner)
        .setName(databaseName).setCreationTime(hmDatabaseArgs.getCreationTime())
        .build();

    OMDatabaseSetOwnerResponse omDatabaseSetOwnerResponse =
        new OMDatabaseSetOwnerResponse(omResponse, oldOwner,  oldOwnerDatabaseList,
            newOwnerDatabaseList, newOwnerDatabaseArgs);

    omDatabaseCreateResponse.addToDBBatch(omMetadataManager, batchOperation);
    omDatabaseSetOwnerResponse.addToDBBatch(omMetadataManager, batchOperation);

    // Do manual commit and see whether addToBatch is successful or not.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);


    Assert.assertEquals(1,
        omMetadataManager.countRowsInTable(omMetadataManager.getDatabaseTable()));

    Table.KeyValue<String, HmDatabaseArgs> keyValue =
        omMetadataManager.getDatabaseTable().iterator().next();

    Assert.assertEquals(omMetadataManager.getDatabaseKey(databaseName),
        keyValue.getKey());
    Assert.assertEquals(newOwnerDatabaseArgs, keyValue.getValue());

    Assert.assertEquals(databaseList,
        omMetadataManager.getUserTableDb().get(
            omMetadataManager.getUserKey(newOwner)));
  }

  @Test
  public void testAddToDBBatchNoOp() throws Exception {

    OMResponse omResponse = OMResponse.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.SetDatabaseProperty)
        .setStatus(OzoneManagerProtocolProtos.Status.DATABASE_NOT_FOUND)
        .setSuccess(false)
        .setCreateDatabaseResponse(CreateDatabaseResponse.getDefaultInstance())
        .build();

    OMDatabaseSetOwnerResponse omDatabaseSetOwnerResponse =
        new OMDatabaseSetOwnerResponse(omResponse);

    try {
      omDatabaseSetOwnerResponse.checkAndUpdateDB(omMetadataManager,
          batchOperation);
      Assert.assertTrue(omMetadataManager.countRowsInTable(
          omMetadataManager.getDatabaseTable()) == 0);
    } catch (IOException ex) {
      fail("testAddToDBBatchFailure failed");
    }

  }


}
