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

package org.apache.hadoop.hetu.hm;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hetu.hm.helpers.OmDatabaseArgs;
import org.apache.hadoop.hetu.om.OmMetadataManagerImpl;
import org.apache.hadoop.hetu.om.TabletManagerImpl;
import org.apache.hadoop.hetu.om.request.TestOMRequestUtils;
import org.apache.hadoop.hetu.security.OzoneBlockTokenSecretManager;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.helpers.OmTabletArgs;
import org.apache.hadoop.ozone.om.helpers.OmTabletInfo;
import org.apache.hadoop.ozone.om.helpers.OmTabletLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmTabletLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OzoneTabletStatus;
import org.apache.hadoop.util.Time;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit test tablet manager.
 */
public class TestTabletManagerUnit {

  private OzoneConfiguration configuration;
  private OmMetadataManagerImpl metadataManager;
  private StorageContainerLocationProtocol containerClient;
  private TabletManagerImpl tabletManager;

  private Instant startDate;
  private File testDir;
  private ScmBlockLocationProtocol blockClient;

  @Before
  public void setup() throws IOException {
    configuration = new OzoneConfiguration();
    testDir = GenericTestUtils.getRandomizedTestDir();
    configuration.set(HddsConfigKeys.OZONE_METADATA_DIRS,
        testDir.toString());
    metadataManager = new OmMetadataManagerImpl(configuration);
    containerClient = Mockito.mock(StorageContainerLocationProtocol.class);
    blockClient = Mockito.mock(ScmBlockLocationProtocol.class);
    tabletManager = new TabletManagerImpl(
        blockClient, containerClient, metadataManager, configuration,
        "omtest", Mockito.mock(OzoneBlockTokenSecretManager.class));

    startDate = Instant.now();
  }

  @After
  public void cleanup() throws Exception {
    metadataManager.stop();
    FileUtils.deleteDirectory(testDir);
  }

  @Test
  public void testLookupTabletWithDnFailure() throws Exception {
    final DatanodeDetails dnOne = MockDatanodeDetails.randomDatanodeDetails();
    final DatanodeDetails dnTwo = MockDatanodeDetails.randomDatanodeDetails();
    final DatanodeDetails dnThree = MockDatanodeDetails.randomDatanodeDetails();

    final DatanodeDetails dnFour = MockDatanodeDetails.randomDatanodeDetails();
    final DatanodeDetails dnFive = MockDatanodeDetails.randomDatanodeDetails();
    final DatanodeDetails dnSix = MockDatanodeDetails.randomDatanodeDetails();

    final Pipeline pipelineOne = Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setReplicationConfig(
            new RatisReplicationConfig(ReplicationFactor.THREE))
        .setState(Pipeline.PipelineState.OPEN)
        .setLeaderId(dnOne.getUuid())
        .setNodes(Arrays.asList(dnOne, dnTwo, dnThree))
        .build();

    final Pipeline pipelineTwo = Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setReplicationConfig(
            new RatisReplicationConfig(ReplicationFactor.THREE))
        .setState(Pipeline.PipelineState.OPEN)
        .setLeaderId(dnFour.getUuid())
        .setNodes(Arrays.asList(dnFour, dnFive, dnSix))
        .build();

    List<Long> containerIDs = new ArrayList<>();
    containerIDs.add(1L);

    List<ContainerWithPipeline> cps = new ArrayList<>();
    ContainerInfo ci = Mockito.mock(ContainerInfo.class);
    when(ci.getContainerID()).thenReturn(1L);
    cps.add(new ContainerWithPipeline(ci, pipelineTwo));

    when(containerClient.getContainerWithPipelineBatch(containerIDs))
        .thenReturn(cps);

    final String databaseName = "databaseOne";
    final String adminName = "admin";
    final String ownerName = "admin";
    final String tableName = "tableOne";
    final String partitionName = "op-20100201";
    final String tabletName = "tabletOne";

    OmDatabaseArgs omDatabaseArgs = OmDatabaseArgs.newBuilder()
            .setName(databaseName)
            .setAdminName(adminName)
            .setOwnerName(ownerName)
            .build();
    TestOMRequestUtils.addDatabaseToOM(metadataManager, omDatabaseArgs);
    TestOMRequestUtils.addMetaTableToOM(metadataManager, databaseName, tableName);
    TestOMRequestUtils.addPartitionToOM(metadataManager, databaseName, tableName, partitionName);

    final OmTabletLocationInfo tabletLocationInfo = new OmTabletLocationInfo.Builder()
        .setBlockID(new BlockID(1L, 1L))
        .setPipeline(pipelineOne)
        .setOffset(0)
        .setLength(256000)
        .build();

    final OmTabletInfo tabletInfo = new OmTabletInfo.Builder()
        .setDatabaseName(databaseName)
        .setTableName(tableName)
        .setPartitionName(partitionName)
        .setTabletName(tabletName)
        .setOmTabletLocationInfos(singletonList(
            new OmTabletLocationInfoGroup(0,
                singletonList(tabletLocationInfo))))
        .setCreationTime(Time.now())
        .setModificationTime(Time.now())
        .setDataSize(256000)
        .setReplicationType(ReplicationType.RATIS)
        .setReplicationFactor(ReplicationFactor.THREE)
        .build();
    TestOMRequestUtils.addTabletToOM(metadataManager, tabletInfo);

    final OmTabletArgs.Builder tabletArgs = new OmTabletArgs.Builder()
        .setDatabaseName(databaseName)
        .setTableName(tableName)
        .setPartitionName(partitionName)
        .setTabletName(tabletName);

    final OmTabletInfo newTabletInfo = tabletManager
        .lookupTablet(tabletArgs.build(), "test");

    final OmTabletLocationInfo newBlockLocation = newTabletInfo
        .getLatestVersionLocations().getBlocksLatestVersionOnly().get(0);

    Assert.assertEquals(1L, newBlockLocation.getContainerID());
    Assert.assertEquals(1L, newBlockLocation
        .getBlockID().getLocalID());
    Assert.assertEquals(pipelineTwo.getId(),
        newBlockLocation.getPipeline().getId());
    Assert.assertTrue(newBlockLocation.getPipeline()
        .getNodes().contains(dnFour));
    Assert.assertTrue(newBlockLocation.getPipeline()
        .getNodes().contains(dnFive));
    Assert.assertTrue(newBlockLocation.getPipeline()
        .getNodes().contains(dnSix));
  }

  @Test
  public void listTabletStatus() throws Exception {
    String database = "db1";
    String table = "table1";
    String partition = "ds-202001";
    String tabletPrefix = "tablet";
    String client = "client.host";

    TestOMRequestUtils.addDatabaseToDB(database, OzoneConsts.OZONE,
        metadataManager);

    TestOMRequestUtils.addMetaTableToDB(database, table, metadataManager);

    TestOMRequestUtils.addPartitionToDB(database, table, partition, metadataManager);

    final Pipeline pipeline = MockPipeline.createPipeline(3);
    final List<String> nodes = pipeline.getNodes().stream()
        .map(DatanodeDetails::getUuidString)
        .collect(toList());

    List<Long> containerIDs = new ArrayList<>();
    List<ContainerWithPipeline> containersWithPipeline = new ArrayList<>();
    for (long i = 1; i <= 10; i++) {
      final OmTabletLocationInfo tabletLocationInfo = new OmTabletLocationInfo.Builder()
          .setBlockID(new BlockID(i, 1L))
          .setPipeline(pipeline)
          .setOffset(0)
          .setLength(256000)
          .build();

      ContainerInfo containerInfo = new ContainerInfo.Builder()
          .setContainerID(i)
          .build();
      containersWithPipeline.add(
          new ContainerWithPipeline(containerInfo, pipeline));
      containerIDs.add(i);

      OmTabletInfo tabletInfo = new OmTabletInfo.Builder()
          .setDatabaseName(database)
          .setTableName(table)
          .setPartitionName(partition)
          .setCreationTime(Time.now())
          .setOmTabletLocationInfos(singletonList(
              new OmTabletLocationInfoGroup(0, new ArrayList<>())))
          .setReplicationFactor(ReplicationFactor.THREE)
          .setReplicationType(ReplicationType.RATIS)
          .setTabletName(tabletPrefix + i)
          .setObjectID(i)
          .setUpdateID(i)
          .build();
      tabletInfo.appendNewBlocks(singletonList(tabletLocationInfo), false);
      TestOMRequestUtils.addTabletToOM(metadataManager, tabletInfo);
    }

    when(containerClient.getContainerWithPipelineBatch(containerIDs))
        .thenReturn(containersWithPipeline);

    OmTabletArgs.Builder builder = new OmTabletArgs.Builder()
        .setDatabaseName(database)
        .setTableName(table)
        .setPartitionName(partition)
        .setTabletName("")
        .setSortDatanodesInPipeline(true);
    List<OzoneTabletStatus> tabletStatusList =
        tabletManager.listStatus(builder.build(),
            null, Long.MAX_VALUE, client);

    Assert.assertEquals(10, tabletStatusList.size());
    verify(containerClient).getContainerWithPipelineBatch(containerIDs);
    verify(blockClient).sortDatanodes(nodes, client);
  }

  @Test
  public void sortDatanodes() throws Exception {
    // GIVEN
    String client = "anyhost";
    int pipelineCount = 3;
    int tabletsPerPipeline = 5;
    OmTabletInfo[] tabletInfos = new OmTabletInfo[pipelineCount * tabletsPerPipeline];
    List<List<String>> expectedSortDatanodesInvocations = new ArrayList<>();
    Map<Pipeline, List<DatanodeDetails>> expectedSortedNodes = new HashMap<>();
    int ki = 0;
    for (int p = 0; p < pipelineCount; p++) {
      final Pipeline pipeline = MockPipeline.createPipeline(3);
      final List<String> nodes = pipeline.getNodes().stream()
          .map(DatanodeDetails::getUuidString)
          .collect(toList());
      expectedSortDatanodesInvocations.add(nodes);
      final List<DatanodeDetails> sortedNodes = pipeline.getNodes().stream()
          .sorted(comparing(DatanodeDetails::getUuidString))
          .collect(toList());
      expectedSortedNodes.put(pipeline, sortedNodes);

      when(blockClient.sortDatanodes(nodes, client))
          .thenReturn(sortedNodes);

      for (int i = 1; i <= tabletsPerPipeline; i++) {
        OmTabletLocationInfo tabletLocationInfo = new OmTabletLocationInfo.Builder()
            .setBlockID(new BlockID(i, 1L))
            .setPipeline(pipeline)
            .setOffset(0)
            .setLength(256000)
            .build();

        OmTabletInfo tabletInfo = new OmTabletInfo.Builder()
            .setOmTabletLocationInfos(Arrays.asList(
                new OmTabletLocationInfoGroup(0, emptyList()),
                new OmTabletLocationInfoGroup(1, singletonList(tabletLocationInfo))))
            .build();
        tabletInfos[ki++] = tabletInfo;
      }
    }

    // WHEN
    tabletManager.sortDatanodes(client, tabletInfos);

    // THEN
    // verify all tablet info locations got updated
    for (OmTabletInfo tabletInfo : tabletInfos) {
      OmTabletLocationInfoGroup locations = tabletInfo.getLatestVersionLocations();
      Assert.assertNotNull(locations);
      for (OmTabletLocationInfo locationInfo : locations.getLocationList()) {
        Pipeline pipeline = locationInfo.getPipeline();
        List<DatanodeDetails> expectedOrder = expectedSortedNodes.get(pipeline);
        Assert.assertEquals(expectedOrder, pipeline.getNodesInOrder());
      }
    }

    // expect one invocation per pipeline
    for (List<String> nodes : expectedSortDatanodesInvocations) {
      verify(blockClient).sortDatanodes(nodes, client);
    }
  }

}
