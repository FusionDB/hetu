/*
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
package org.apache.hadoop.ozone.container.lstore.impl;

import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hetu.photon.ClientTestUtil;
import org.apache.hadoop.hetu.photon.helpers.PartialRow;
import org.apache.hadoop.hetu.photon.operation.OperationType;
import org.apache.hadoop.hetu.photon.operation.request.InsertOperationRequest;
import org.apache.hadoop.hetu.photon.operation.request.OperationRequest;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.impl.ChunkLayOutVersion;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.volume.VolumeIOStats;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.lstore.SegmentLayoutTestInfo;
import org.apache.hadoop.ozone.tablet.lstore.LStoreContainer;
import org.apache.hadoop.ozone.tablet.lstore.LStoreContainerData;
import org.apache.hadoop.ozone.tablet.lstore.impl.BlockManagerImpl;
import org.apache.hadoop.ozone.tablet.lstore.interfaces.BlockManager;
import org.apache.hadoop.ozone.tablet.lstore.interfaces.ChunkManager;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.File;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;

/**
 * Helpers for ChunkManager implementation tests.
 */
public abstract class AbstractTestSegmentManager {

  private HddsVolume hddsVolume;
  private LStoreContainerData lStoreContainerData;
  private LStoreContainer lStoreContainer;
  private BlockID blockID;
  private ChunkInfo chunkInfo;
  private ByteBuffer data;
  private PartialRow partialRow;
  private byte[] header;
  private BlockManager blockManager;

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  protected abstract SegmentLayoutTestInfo getStrategy();

  protected ChunkManager createTestSubject() {
    blockManager = new BlockManagerImpl(new OzoneConfiguration());
    return getStrategy().createChunkManager(true, blockManager);
  }

  @Before
  public final void setUp() throws Exception {
    OzoneConfiguration config = new OzoneConfiguration();
    getStrategy().updateConfig(config);
    UUID datanodeId = UUID.randomUUID();
    hddsVolume = new HddsVolume.Builder(folder.getRoot()
        .getAbsolutePath()).conf(config).datanodeUuid(datanodeId
        .toString()).build();

    VolumeSet volumeSet = mock(MutableVolumeSet.class);

    RoundRobinVolumeChoosingPolicy volumeChoosingPolicy =
        mock(RoundRobinVolumeChoosingPolicy.class);
    Mockito.when(volumeChoosingPolicy.chooseVolume(anyList(), anyLong()))
        .thenReturn(hddsVolume);

    lStoreContainerData = new LStoreContainerData(1L,
        ChunkLayOutVersion.getConfiguredVersion(config),
        (long) StorageUnit.GB.toBytes(5), UUID.randomUUID().toString(),
        datanodeId.toString());

    lStoreContainer = new LStoreContainer(lStoreContainerData, config);

    lStoreContainer.create(volumeSet, volumeChoosingPolicy,
        UUID.randomUUID().toString());

    partialRow = ClientTestUtil.getPartialRowWithAllTypes();
    OperationRequest operation = OperationRequest.newBuilder()
            .setOperationType(OperationType.INSERT)
            .setInsertOperationRequest(new InsertOperationRequest(partialRow))
            .build();
    header = operation.toProto().toByteArray();
    data = ByteBuffer.allocate(header.length)
        .put(header);
    rewindBufferToDataStart();

    // Creating BlockData
    blockID = new BlockID(1L, 1L);
    chunkInfo = new ChunkInfo(String.format("%d.data.%d", blockID
        .getLocalID(), 0), 0, partialRow.toProtobuf().getSerializedSize());
  }

  protected DispatcherContext getDispatcherContext() {
    return new DispatcherContext.Builder().build();
  }

  protected Buffer rewindBufferToDataStart() {
    byte[] row = partialRow.toProtobuf().toByteArray();
    return ByteBuffer.allocate(row.length)
            .put(row).position(0);
  }

  protected void checkChunkFileCount(int expected) {
    //As in Setup, we try to create container, these paths should exist.
    String path = lStoreContainerData.getChunksPath();
    assertNotNull(path);

    File dir = new File(path);
    assertTrue(dir.exists());

    File[] files = dir.listFiles();
    assertNotNull(files);
    assertEquals(expected, files.length);
  }

  protected void checkWriteIOStats(long length, long opCount) {
    VolumeIOStats volumeIOStats = hddsVolume.getVolumeIOStats();
    assertEquals(length, volumeIOStats.getWriteBytes());
    assertEquals(opCount, volumeIOStats.getWriteOpCount());
  }

  protected void checkReadIOStats(long length, long opCount) {
    VolumeIOStats volumeIOStats = hddsVolume.getVolumeIOStats();
    assertEquals(length, volumeIOStats.getReadBytes());
    assertEquals(opCount, volumeIOStats.getReadOpCount());
  }

  protected HddsVolume getHddsVolume() {
    return hddsVolume;
  }

  protected LStoreContainerData getLStoreContainerData() {
    return lStoreContainerData;
  }

  protected LStoreContainer getLStoreContainer() {
    return lStoreContainer;
  }

  protected BlockID getBlockID() {
    return blockID;
  }

  protected ChunkInfo getChunkInfo() {
    return chunkInfo;
  }

  protected ByteBuffer getData() {
    return data;
  }

  protected long getRealDataSerializedSize() {
    return partialRow.toProtobuf().getSerializedSize();
  }

  protected BlockManager getBlockManager() {
    return blockManager;
  }
}
