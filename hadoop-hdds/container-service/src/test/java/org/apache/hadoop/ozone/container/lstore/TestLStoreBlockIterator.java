/**
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

package org.apache.hadoop.ozone.container.lstore;

import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.utils.MetadataKeyFilters;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.impl.ChunkLayOutVersion;
import org.apache.hadoop.ozone.container.common.interfaces.BlockIterator;
import org.apache.hadoop.ozone.container.common.utils.ReferenceCountedDB;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.tablet.lstore.LStoreContainer;
import org.apache.hadoop.ozone.tablet.lstore.LStoreContainerData;
import org.apache.hadoop.ozone.tablet.lstore.helpers.BlockUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_KEY;
import static org.apache.hadoop.ozone.container.common.impl.ChunkLayOutVersion.FILE_PER_BLOCK;
import static org.apache.hadoop.ozone.container.common.impl.ChunkLayOutVersion.FILE_PER_CHUNK;
import static org.apache.hadoop.ozone.container.common.impl.ChunkLayOutVersion.FILE_PER_SEGMENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * This class is used to test LStore container block iterator.
 */
@RunWith(Parameterized.class)
public class TestLStoreBlockIterator {

  private static final long CONTAINER_ID = 105L;

  private LStoreContainer container;
  private LStoreContainerData containerData;
  private MutableVolumeSet volumeSet;
  private OzoneConfiguration conf;
  private File testRoot;
  private ReferenceCountedDB db;
  private final ChunkLayOutVersion layout;

  public TestLStoreBlockIterator(ChunkLayOutVersion layout) {
    this.layout = layout;
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {FILE_PER_CHUNK},
        {FILE_PER_BLOCK},
        {FILE_PER_SEGMENT}
    });
  }

  @Before
  public void setUp() throws Exception {
    testRoot = GenericTestUtils.getRandomizedTestDir();
    conf = new OzoneConfiguration();
    conf.set(HDDS_DATANODE_DIR_KEY, testRoot.getAbsolutePath());
    volumeSet = new MutableVolumeSet(UUID.randomUUID().toString(), conf, null);

    containerData = new LStoreContainerData(105L,
            layout,
            (long) StorageUnit.GB.toBytes(1), UUID.randomUUID().toString(),
            UUID.randomUUID().toString());
    // Init the container.
    container = new LStoreContainer(containerData, conf);
    container.create(volumeSet, new RoundRobinVolumeChoosingPolicy(), UUID
            .randomUUID().toString());
    db = BlockUtils.getDB(containerData, conf);
  }


  @After
  public void tearDown() throws Exception {
    db.close();
    db.cleanup();
    volumeSet.shutdown();
    FileUtil.fullyDelete(testRoot);
  }

  @Test
  public void testLStoreBlockIteratorWithMixedBlocks() throws Exception {
    int deletingBlocks = 5;
    int normalBlocks = 5;
    Map<String, List<Long>> blockIDs = createContainerWithBlocks(CONTAINER_ID,
            normalBlocks, deletingBlocks);

    // Default filter used is all unprefixed blocks.
    List<Long> unprefixedBlockIDs = blockIDs.get("");
    try(BlockIterator<BlockData> lStoreBlockIterator =
                db.getStore().getBlockIterator()) {

      Iterator<Long> blockIDIter = unprefixedBlockIDs.iterator();
      while (lStoreBlockIterator.hasNext()) {
        BlockData blockData = lStoreBlockIterator.nextBlock();
        assertEquals(blockData.getLocalID(), (long)blockIDIter.next());
      }
      assertFalse(lStoreBlockIterator.hasNext());
      assertFalse(blockIDIter.hasNext());

      lStoreBlockIterator.seekToFirst();
      blockIDIter = unprefixedBlockIDs.iterator();
      while (lStoreBlockIterator.hasNext()) {
        BlockData blockData = lStoreBlockIterator.nextBlock();
        assertEquals(blockData.getLocalID(), (long)blockIDIter.next());
      }
      assertFalse(lStoreBlockIterator.hasNext());
      assertFalse(blockIDIter.hasNext());

      try {
        lStoreBlockIterator.nextBlock();
      } catch (NoSuchElementException ex) {
        GenericTestUtils.assertExceptionContains("Block Iterator reached end " +
            "for ContainerID " + CONTAINER_ID, ex);
      }
    }
  }

  @Test
  public void testLStoreBlockIteratorWithNextBlock() throws Exception {
    List<Long> blockIDs = createContainerWithBlocks(CONTAINER_ID, 2);
    try(BlockIterator<BlockData> lStoreBlockIterator =
                db.getStore().getBlockIterator()) {
      assertEquals((long)blockIDs.get(0),
              lStoreBlockIterator.nextBlock().getLocalID());
      assertEquals((long)blockIDs.get(1),
              lStoreBlockIterator.nextBlock().getLocalID());

      try {
        lStoreBlockIterator.nextBlock();
      } catch (NoSuchElementException ex) {
        GenericTestUtils.assertExceptionContains("Block Iterator reached end " +
            "for ContainerID " + CONTAINER_ID, ex);
      }
    }
  }

  @Test
  public void testLStoreBlockIteratorWithHasNext() throws Exception {
    List<Long> blockIDs = createContainerWithBlocks(CONTAINER_ID, 2);
    try(BlockIterator<BlockData> blockIter =
                db.getStore().getBlockIterator()) {

      // Even calling multiple times hasNext() should not move entry forward.
      assertTrue(blockIter.hasNext());
      assertTrue(blockIter.hasNext());
      assertTrue(blockIter.hasNext());
      assertTrue(blockIter.hasNext());
      assertTrue(blockIter.hasNext());
      assertEquals((long)blockIDs.get(0),
              blockIter.nextBlock().getLocalID());

      assertTrue(blockIter.hasNext());
      assertTrue(blockIter.hasNext());
      assertTrue(blockIter.hasNext());
      assertTrue(blockIter.hasNext());
      assertTrue(blockIter.hasNext());
      assertEquals((long)blockIDs.get(1), blockIter.nextBlock().getLocalID());

      blockIter.seekToFirst();
      assertEquals((long)blockIDs.get(0), blockIter.nextBlock().getLocalID());
      assertEquals((long)blockIDs.get(1), blockIter.nextBlock().getLocalID());

      try {
        blockIter.nextBlock();
      } catch (NoSuchElementException ex) {
        GenericTestUtils.assertExceptionContains("Block Iterator reached end " +
            "for ContainerID " + CONTAINER_ID, ex);
      }
    }
  }

  @Test
  public void testLStoreBlockIteratorWithFilter() throws Exception {
    int normalBlocks = 5;
    int deletingBlocks = 5;
    Map<String, List<Long>> blockIDs = createContainerWithBlocks(CONTAINER_ID,
            normalBlocks, deletingBlocks);
    try(BlockIterator<BlockData> lStoreBlockIterator =
                db.getStore().getBlockIterator(
                        MetadataKeyFilters.getDeletingKeyFilter())) {
      List<Long> deletingBlockIDs =
              blockIDs.get(OzoneConsts.DELETING_KEY_PREFIX);
      int counter = 0;
      while (lStoreBlockIterator.hasNext()) {
        BlockData blockData = lStoreBlockIterator.nextBlock();
        assertEquals((long)deletingBlockIDs.get(counter),
                blockData.getLocalID());
        counter++;
      }

      assertEquals(deletingBlocks, counter);
    }
  }

  @Test
  public void testLStoreBlockIteratorWithOnlyDeletedBlocks() throws
      Exception {
    createContainerWithBlocks(CONTAINER_ID, 0, 5);
    try(BlockIterator<BlockData> lStoreBlockIterator =
                db.getStore().getBlockIterator()) {
      //As all blocks are deleted blocks, blocks does not match with normal key
      // filter.
      assertFalse(lStoreBlockIterator.hasNext());
    }
  }

  /**
   * Due to RocksDB internals, prefixed keys may be grouped all at the
   * beginning or end of the key iteration, depending on the serialization
   * used. Keys of the same prefix are grouped
   * together. This method runs the same set of tests on the iterator first
   * positively filtering one prefix, and then positively filtering
   * a second prefix. If the sets of keys with prefix one, prefix
   * two, and no prefixes are not empty, it follows that the filter will
   * encounter both of the following cases:
   *
   * 1. A failing key followed by a passing key.
   * 2. A passing key followed by a failing key.
   *
   * Note that with the current block data table implementation, there is
   * only ever one type of prefix. This test adds a dummy second prefix type
   * to ensure that the iterator will continue to work if more prefixes are
   * added in the future.
   *
   * @throws Exception
   */
  @Test
  public void testLStoreBlockIteratorWithAdvancedFilter() throws
          Exception {
    // Block data table currently only uses one prefix type.
    // Introduce a second prefix type to make sure the iterator functions
    // correctly if more prefixes were to be added in the future.
    final String secondPrefix = "#FOOBAR#";
    Map<String, Integer> prefixCounts = new HashMap<>();
    prefixCounts.put(OzoneConsts.DELETING_KEY_PREFIX, 3);
    prefixCounts.put("", 3);
    prefixCounts.put(secondPrefix, 3);

    Map<String, List<Long>> blockIDs = createContainerWithBlocks(CONTAINER_ID,
            prefixCounts);
    // Test deleting filter.
    testWithFilter(MetadataKeyFilters.getDeletingKeyFilter(),
            blockIDs.get(OzoneConsts.DELETING_KEY_PREFIX));

    // Test arbitrary filter.
    MetadataKeyFilters.KeyPrefixFilter secondFilter =
            new MetadataKeyFilters.KeyPrefixFilter()
            .addFilter(secondPrefix);
    testWithFilter(secondFilter, blockIDs.get(secondPrefix));
  }

  /**
   * Helper method to run some iterator tests with a provided filter.
   */
  private void testWithFilter(MetadataKeyFilters.KeyPrefixFilter filter,
                              List<Long> expectedIDs) throws Exception {
    try(BlockIterator<BlockData> iterator =
                db.getStore().getBlockIterator(filter)) {
      // Test seek.
      iterator.seekToFirst();
      long firstID = iterator.nextBlock().getLocalID();
      assertEquals(expectedIDs.get(0).longValue(), firstID);
      assertTrue(iterator.hasNext());

      // Test atypical iteration use.
      iterator.seekToFirst();
      int numIDsSeen = 0;
      for (long id: expectedIDs) {
        assertEquals(iterator.nextBlock().getLocalID(), id);
        numIDsSeen++;

        // Test that iterator can handle sporadic hasNext() calls.
        if (id % 2 == 0 && numIDsSeen < expectedIDs.size()) {
          assertTrue(iterator.hasNext());
        }
      }

      assertFalse(iterator.hasNext());
    }
  }

  /**
   * Creates a container with specified number of unprefixed blocks.
   * @param containerId
   * @param unprefixedBlocks
   * @return The list of block IDs of normal blocks that were created.
   * @throws Exception
   */
  private List<Long> createContainerWithBlocks(long containerId,
            int unprefixedBlocks) throws Exception {
    return createContainerWithBlocks(containerId, unprefixedBlocks, 0).get("");
  }

  /**
   * Creates a container with specified number of unprefixed blocks and deleted
   * blocks.
   * @param containerId
   * @param unprefixedBlocks
   * @param deletingBlocks
   * @return Each key prefix mapped to the sorted list of block IDs of blocks
   * created whose keys had that prefix.
   * @throws Exception
   */
  private Map<String, List<Long>> createContainerWithBlocks(long containerId,
            int unprefixedBlocks, int deletingBlocks) throws Exception {

    Map<String, Integer> prefixes = new HashMap<>();
    prefixes.put("", unprefixedBlocks);
    prefixes.put(OzoneConsts.DELETING_KEY_PREFIX, deletingBlocks);
    return createContainerWithBlocks(containerId, prefixes);
  }

  /**
   * Creates a container with the provided container ID based on
   * {@link TestLStoreBlockIterator#conf}. For each prefix specified in
   * {@code prefixCounts}, the specified number of keys with that prefix will
   * be created.
   * <p>
   * Returns each prefix mapped to a list of the block IDs that were created
   * for that prefix. Each block ID list will be sorted for each prefix.
   *
   * @param containerId The ID of the container to create.
   * @param prefixCounts A map mapping each key prefix to the number of
    * blocks that should be created whose keys have that prefix.
   * @return Each key prefix mapped to the sorted list of block IDs of blocks
   * created whose keys had that prefix.
   * @throws Exception
   */
  private Map<String, List<Long>> createContainerWithBlocks(long containerId,
            Map<String, Integer> prefixCounts) throws Exception {
    // Create required block data.
    Map<String, List<Long>> blockIDs = new HashMap<>();
    try(ReferenceCountedDB metadataStore = BlockUtils.getDB(containerData,
        conf)) {

      List<ContainerProtos.ChunkInfo> chunkList = new ArrayList<>();
      ChunkInfo info = new ChunkInfo("chunkfile", 0, 1024);
      chunkList.add(info.getProtoBufMessage());

      int blockIndex = 0;
      // Maps each prefix to the block IDs of the blocks created with that
      // prefix.
      Table<String, BlockData> blockDataTable =
              metadataStore.getStore().getBlockDataTable();

      for (Map.Entry<String, Integer> entry: prefixCounts.entrySet()) {
        String prefix = entry.getKey();
        blockIDs.put(prefix, new ArrayList<>());
        int numBlocks = entry.getValue();

        for (int i = 0; i < numBlocks; i++) {
          BlockID blockID = new BlockID(containerId, blockIndex);
          blockIndex++;
          BlockData blockData = new BlockData(blockID);
          blockData.setChunks(chunkList);
          String localID = prefix + blockID.getLocalID();
          blockDataTable.put(localID, blockData);
          blockIDs.get(prefix).add(blockID.getLocalID());
        }
      }
    }

    return blockIDs;
  }
}
