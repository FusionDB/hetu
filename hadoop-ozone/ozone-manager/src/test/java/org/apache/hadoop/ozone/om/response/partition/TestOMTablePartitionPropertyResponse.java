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

package org.apache.hadoop.ozone.om.response.partition;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.OmPartitionInfo;
import org.apache.hadoop.ozone.om.helpers.OmTableInfo;
import org.apache.hadoop.ozone.om.response.TestOMResponseUtils;
import org.apache.hadoop.ozone.om.response.table.OMTableSetPropertyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreatePartitionResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.UUID;

/**
 * This class tests OMTableSetPropertyResponse.
 */
public class TestOMTablePartitionPropertyResponse {

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
    String databaseName = "fdb";
    String tableName = "test";
    String partitionName = "p1";
    String partitionValue = "20101011";
    OmPartitionInfo omPartitionInfo = TestOMResponseUtils.createPartition(
            databaseName, tableName, partitionName, partitionValue);
    OMPartitionSetPropertyResponse omPartitionSetPropertyResponse =
        new OMPartitionSetPropertyResponse(OMResponse.newBuilder()
            .setCmdType(OzoneManagerProtocolProtos.Type.SetPartitionProperty)
            .setStatus(OzoneManagerProtocolProtos.Status.OK)
            .setCreatePartitionResponse(
                CreatePartitionResponse.newBuilder().build()).build(),
            omPartitionInfo);

    omPartitionSetPropertyResponse.addToDBBatch(omMetadataManager, batchOperation);

    // Do manual commit and see whether addToBatch is successful or not.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    Assert.assertEquals(1,
        omMetadataManager.countRowsInTable(omMetadataManager.getPartitionTable()));

    Table.KeyValue<String, OmPartitionInfo> keyValue =
        omMetadataManager.getPartitionTable().iterator().next();

    Assert.assertEquals(omMetadataManager.getPartitionKey(databaseName,
        tableName, partitionName), keyValue.getKey());
    Assert.assertEquals(omPartitionInfo.toString(), keyValue.getValue().toString());
  }


}
