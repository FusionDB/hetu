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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.lstore.impl;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hetu.photon.WriteType;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.impl.ChunkLayOutVersion;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext;
import org.apache.hadoop.ozone.container.common.volume.VolumeIOStats;
import org.apache.hadoop.ozone.container.lstore.SegmentLayoutTestInfo;
import org.apache.hadoop.ozone.tablet.lstore.LStoreContainer;
import org.apache.hadoop.ozone.tablet.lstore.helpers.LStoreUtils;
import org.apache.hadoop.ozone.tablet.lstore.interfaces.ChunkManager;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test for FilePerChunkStrategy.
 */
public class TestFilePerSegmentStrategy extends CommonSegmentManagerTestCases {

  @Override
  protected SegmentLayoutTestInfo getStrategy() {
    return SegmentLayoutTestInfo.FILE_PER_SEGMENT;
  }

  @Test
  public void testWriteChunkStageWriteAndCommit() throws Exception {
    ChunkManager chunkManager = createTestSubject();

    checkChunkFileCount(0);

    // As no chunks are written to the volume writeBytes should be 0
    checkWriteIOStats(0, 0);
    LStoreContainer container = getLStoreContainer();
    BlockID blockID = getBlockID();
    ChunkInfo chunkInfo = getChunkInfo();
    chunkManager.writeChunk(container, blockID, chunkInfo, WriteType.INSERT, getData(),
        new DispatcherContext.Builder()
            .setStage(DispatcherContext.WriteChunkStage.WRITE_DATA).build());
    // Now a chunk file is being written with Stage WRITE_DATA, so it should
    // create a temporary chunk file.
    checkChunkFileCount(1);

    long term = 0;
    long index = 0;
    File chunkFile = ChunkLayOutVersion.FILE_PER_SEGMENT
        .getChunkFile(container.getContainerData(), blockID, chunkInfo);

    checkWriteIOStats(chunkInfo.getLen(), 1);

    chunkManager.writeChunk(container, blockID, chunkInfo, WriteType.INSERT, getData(),
        new DispatcherContext.Builder()
            .setStage(DispatcherContext.WriteChunkStage.COMMIT_DATA).build());

    checkWriteIOStats(chunkInfo.getLen(), 1);

    // Old temp file should have been renamed to chunk file.
    checkChunkFileCount(1);

    // As commit happened, chunk file should exist.
    assertTrue(chunkFile.exists());
  }

  /**
   * Tests that "new datanode" can delete chunks written to "old
   * datanode" by "new client" (ie. where chunk file accidentally created with
   * {@code size = chunk offset + chunk length}, instead of only chunk length).
   */
  @Test
  public void deletesChunkFileWithLengthIncludingOffset() throws Exception {
    // GIVEN
    ChunkManager chunkManager = createTestSubject();
    LStoreContainer container = getLStoreContainer();
    BlockID blockID = getBlockID();
    ChunkInfo chunkInfo = getChunkInfo();
    long offset = 0;

    ChunkInfo oldDatanodeChunkInfo = new ChunkInfo(chunkInfo.getChunkName(),
        offset, chunkInfo.getLen());
    File file = ChunkLayOutVersion.FILE_PER_SEGMENT.getChunkFile(
        container.getContainerData(), blockID, chunkInfo);
    LStoreUtils.writeData(file.toPath(),
        WriteType.INSERT,
        ChunkBuffer.wrap(getData()), chunkInfo.getLen(),
        new VolumeIOStats(), true);
    checkChunkFileCount(1);
    assertTrue(file.exists());
    assertEquals(offset + chunkInfo.getLen(), getRealDataSerializedSize());

    // WHEN
    chunkManager.deleteChunk(container, blockID, oldDatanodeChunkInfo);

    // THEN
    checkChunkFileCount(0);
    assertFalse(file.exists());
  }

}
