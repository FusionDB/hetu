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

package org.apache.hadoop.ozone.om.response.tablet;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.util.Random;
import java.util.UUID;

/**
 * Base test class for tablet response.
 */
@SuppressWarnings("visibilitymodifier")
public class TestOMTabletResponse {
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  protected OMMetadataManager omMetadataManager;
  protected BatchOperation batchOperation;

  protected String databaseName;
  protected String tableName;
  protected String partitionName;
  protected String tabletName;
  protected HddsProtos.ReplicationFactor replicationFactor;
  protected HddsProtos.ReplicationType replicationType;
  protected long clientID;
  protected Random random;

  @Before
  public void setup() throws Exception {
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        folder.newFolder().getAbsolutePath());
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration);
    batchOperation = omMetadataManager.getStore().initBatchOperation();

    databaseName = UUID.randomUUID().toString();
    tableName = UUID.randomUUID().toString();
    partitionName = UUID.randomUUID().toString();
    tabletName = UUID.randomUUID().toString();
    replicationFactor = HddsProtos.ReplicationFactor.ONE;
    replicationType = HddsProtos.ReplicationType.RATIS;
    clientID = 1000L;
    random = new Random();
  }

  @After
  public void stop() {
    Mockito.framework().clearInlineMocks();
    if (batchOperation != null) {
      batchOperation.close();
    }
  }

}
