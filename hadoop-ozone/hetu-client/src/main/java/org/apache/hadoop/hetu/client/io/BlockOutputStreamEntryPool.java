
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
package org.apache.hadoop.hetu.client.io;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ByteStringConversion;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.storage.BufferPool;
import org.apache.hadoop.ozone.om.helpers.OmMultipartCommitUploadPartInfo;
import org.apache.hadoop.ozone.om.helpers.OmTabletArgs;
import org.apache.hadoop.ozone.om.helpers.OmTabletInfo;
import org.apache.hadoop.ozone.om.helpers.OmTabletLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmTabletLocationInfoGroup;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

/**
 * This class manages the stream entries list and handles block allocation
 * from OzoneManager.
 */
public class BlockOutputStreamEntryPool {

  public static final Logger LOG =
      LoggerFactory.getLogger(BlockOutputStreamEntryPool.class);

  private final List<BlockOutputStreamEntry> streamEntries;
  private final OzoneClientConfig config;
  private int currentStreamIndex;
  private final OzoneManagerProtocol omClient;
  private final OmTabletArgs tabletArgs;
  private final XceiverClientFactory xceiverClientFactory;
  private final String requestID;
  private final BufferPool bufferPool;
  private OmMultipartCommitUploadPartInfo commitUploadPartInfo;
  private final long openID;
  private final ExcludeList excludeList;

  @SuppressWarnings({"parameternumber", "squid:S00107"})
  public BlockOutputStreamEntryPool(
      OzoneClientConfig config,
      OzoneManagerProtocol omClient,
      String requestId, HddsProtos.ReplicationFactor factor,
      HddsProtos.ReplicationType type,
      OmTabletInfo info,
      boolean unsafeByteBufferConversion,
      XceiverClientFactory xceiverClientFactory, long openID
  ) {
    this.config = config;
    this.xceiverClientFactory = xceiverClientFactory;
    streamEntries = new ArrayList<>();
    currentStreamIndex = 0;
    this.omClient = omClient;
    this.tabletArgs = new OmTabletArgs.Builder().setDatabaseName(info.getDatabaseName())
        .setTableName(info.getTableName()).setPartitionName(info.getPartitionName())
        .setTabletName(info.getTabletName())
        .setType(type).setFactor(factor).setDataSize(info.getDataSize())
        .build();
    this.requestID = requestId;
    this.openID = openID;
    this.excludeList = new ExcludeList();

    this.bufferPool =
        new BufferPool(config.getStreamBufferSize(),
            (int) (config.getStreamBufferMaxSize() / config
                .getStreamBufferSize()),
            ByteStringConversion
                .createByteBufferConversion(unsafeByteBufferConversion));
  }

  /**
   * A constructor for testing purpose only.
   *
   * @see TabletOutputStream#TabletOutputStream()
   */
  @VisibleForTesting
  BlockOutputStreamEntryPool() {
    streamEntries = new ArrayList<>();
    omClient = null;
    tabletArgs = null;
    xceiverClientFactory = null;
    config =
        new OzoneConfiguration().getObject(OzoneClientConfig.class);
    config.setStreamBufferSize(0);
    config.setStreamBufferMaxSize(0);
    config.setStreamBufferFlushSize(0);
    config.setStreamBufferFlushDelay(false);
    requestID = null;
    int chunkSize = 0;
    bufferPool = new BufferPool(chunkSize, 1);

    currentStreamIndex = 0;
    openID = -1;
    excludeList = new ExcludeList();
  }

  /**
   * When a tablet is opened, it is possible that there are some blocks already
   * allocated to it for this open session. In this case, to make use of these
   * blocks, we need to add these blocks to stream entries. But, a tablet's version
   * also includes blocks from previous versions, we need to avoid adding these
   * old blocks to stream entries, because these old blocks should not be picked
   * for write. To do this, the following method checks that, only those
   * blocks created in this particular open version are added to stream entries.
   *
   * @param version the set of blocks that are pre-allocated.
   * @param openVersion the version corresponding to the pre-allocation.
   * @throws IOException
   */
  public void addPreallocateBlocks(OmTabletLocationInfoGroup version,
    long openVersion) throws IOException {
    // server may return any number of blocks, (0 to any)
    // only the blocks allocated in this open session (block createVersion
    // equals to open session version)
    for (OmTabletLocationInfo subTabletInfo : version.getLocationList(openVersion)) {
      addTabletLocationInfo(subTabletInfo);
    }
  }

  private void addTabletLocationInfo(OmTabletLocationInfo subTabletInfo) {
    Preconditions.checkNotNull(subTabletInfo.getPipeline());
    BlockOutputStreamEntry.Builder builder =
        new BlockOutputStreamEntry.Builder()
            .setBlockID(subTabletInfo.getBlockID())
            .setTablet(tabletArgs.getTabletName())
            .setXceiverClientManager(xceiverClientFactory)
            .setPipeline(subTabletInfo.getPipeline())
            .setConfig(config)
            .setLength(subTabletInfo.getLength())
            .setBufferPool(bufferPool)
            .setToken(subTabletInfo.getToken());
    streamEntries.add(builder.build());
  }

  public List<OmTabletLocationInfo> getLocationInfoList()  {
    List<OmTabletLocationInfo> locationInfoList = new ArrayList<>();
    for (BlockOutputStreamEntry streamEntry : streamEntries) {
      long length = streamEntry.getCurrentPosition();

      // Commit only those blocks to OzoneManager which are not empty
      if (length != 0) {
        OmTabletLocationInfo info =
            new OmTabletLocationInfo.Builder().setBlockID(streamEntry.getBlockID())
                .setLength(streamEntry.getCurrentPosition()).setOffset(0)
                .setToken(streamEntry.getToken())
                .setPipeline(streamEntry.getPipeline()).build();
        locationInfoList.add(info);
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "block written " + streamEntry.getBlockID() + ", length " + length
                + " bcsID " + streamEntry.getBlockID()
                .getBlockCommitSequenceId());
      }
    }
    return locationInfoList;
  }

  /**
   * Discards the subsequent pre allocated blocks and removes the streamEntries
   * from the streamEntries list for the container which is closed.
   * @param containerID id of the closed container
   * @param pipelineId id of the associated pipeline
   */
  void discardPreallocatedBlocks(long containerID, PipelineID pipelineId) {
    // currentStreamIndex < streamEntries.size() signifies that, there are still
    // pre allocated blocks available.

    // This will be called only to discard the next subsequent unused blocks
    // in the streamEntryList.
    if (currentStreamIndex + 1 < streamEntries.size()) {
      ListIterator<BlockOutputStreamEntry> streamEntryIterator =
          streamEntries.listIterator(currentStreamIndex + 1);
      while (streamEntryIterator.hasNext()) {
        BlockOutputStreamEntry streamEntry = streamEntryIterator.next();
        Preconditions.checkArgument(streamEntry.getCurrentPosition() == 0);
        if ((streamEntry.getPipeline().getId().equals(pipelineId)) ||
            (containerID != -1 &&
                streamEntry.getBlockID().getContainerID() == containerID)) {
          streamEntryIterator.remove();
        }
      }
    }
  }

  List<BlockOutputStreamEntry> getStreamEntries() {
    return streamEntries;
  }

  XceiverClientFactory getXceiverClientFactory() {
    return xceiverClientFactory;
  }

  String getTabletName() {
    return tabletArgs.getTabletName();
  }

  long getTabletLength() {
    return streamEntries.stream().mapToLong(
        BlockOutputStreamEntry::getCurrentPosition).sum();
  }
  /**
   * Contact OM to get a new block. Set the new block with the index (e.g.
   * first block has index = 0, second has index = 1 etc.)
   *
   * The returned block is made to new BlockOutputStreamEntry to write.
   *
   * @throws IOException
   */
  private void allocateNewBlock() throws IOException {
    if (!excludeList.isEmpty()) {
      LOG.debug("Allocating block with {}", excludeList);
    }
    OmTabletLocationInfo subTabletInfo =
        omClient.allocateTablet(tabletArgs, openID, excludeList);
    addTabletLocationInfo(subTabletInfo);
  }


  void commitTablet(long offset) throws IOException {
    if (tabletArgs != null) {
      // in test, this could be null
      long length = getTabletLength();
      Preconditions.checkArgument(offset == length);
      tabletArgs.setDataSize(length);
      tabletArgs.setLocationInfoList(getLocationInfoList());
      omClient.commitTablet(tabletArgs, openID);
    } else {
      LOG.warn("Closing TabletOutputStream, but tablet args is null");
    }
  }

  public BlockOutputStreamEntry getCurrentStreamEntry() {
    if (streamEntries.isEmpty() || streamEntries.size() <= currentStreamIndex) {
      return null;
    } else {
      return streamEntries.get(currentStreamIndex);
    }
  }

  BlockOutputStreamEntry allocateBlockIfNeeded() throws IOException {
    BlockOutputStreamEntry streamEntry = getCurrentStreamEntry();
    if (streamEntry != null && streamEntry.isClosed()) {
      // a stream entry gets closed either by :
      // a. If the stream gets full
      // b. it has encountered an exception
      currentStreamIndex++;
    }
    if (streamEntries.size() <= currentStreamIndex) {
      Preconditions.checkNotNull(omClient);
      // allocate a new block, if a exception happens, log an error and
      // throw exception to the caller directly, and the write fails.
      allocateNewBlock();
    }
    // in theory, this condition should never violate due the check above
    // still do a sanity check.
    Preconditions.checkArgument(currentStreamIndex < streamEntries.size());
    return streamEntries.get(currentStreamIndex);
  }

  long computeBufferData() {
    return bufferPool.computeBufferData();
  }

  void cleanup() {
    if (excludeList != null) {
      excludeList.clear();
    }
    if (bufferPool != null) {
      bufferPool.clearBufferPool();
    }

    if (streamEntries != null) {
      streamEntries.clear();
    }
  }

  public OmMultipartCommitUploadPartInfo getCommitUploadPartInfo() {
    return commitUploadPartInfo;
  }

  public ExcludeList getExcludeList() {
    return excludeList;
  }

  boolean isEmpty() {
    return streamEntries.isEmpty();
  }
}
