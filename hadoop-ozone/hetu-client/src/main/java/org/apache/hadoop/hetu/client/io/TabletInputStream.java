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
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.fs.CanUnbuffer;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.BlockInputStream;
import org.apache.hadoop.hdds.scm.storage.ByteArrayReader;
import org.apache.hadoop.hdds.scm.storage.ByteBufferReader;
import org.apache.hadoop.hdds.scm.storage.ByteReaderStrategy;
import org.apache.hadoop.ozone.client.io.LengthInputStream;
import org.apache.hadoop.ozone.om.helpers.OmTabletInfo;
import org.apache.hadoop.ozone.om.helpers.OmTabletLocationInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Maintaining a list of BlockInputStream. Read based on offset.
 */
public class TabletInputStream extends InputStream
    implements Seekable, CanUnbuffer, ByteBufferReadable {

  private static final Logger LOG =
      LoggerFactory.getLogger(TabletInputStream.class);

  private static final int EOF = -1;

  private String tablet;
  private long length = 0;
  private boolean closed = false;

  // List of BlockInputStreams, one for each block in the tablet
  private final List<BlockInputStream> blockStreams;

  // blockOffsets[i] stores the index of the first data byte in
  // blockStream w.r.t the tablet data.
  // For example, let’s say the block size is 200 bytes and block[0] stores
  // data from indices 0 - 199, block[1] from indices 200 - 399 and so on.
  // Then, blockOffset[0] = 0 (the offset of the first byte of data in
  // block[0]), blockOffset[1] = 200 and so on.
  private long[] blockOffsets = null;

  // Index of the blockStream corresponding to the current position of the
  // TabletInputStream i.e. offset of the data to be read next
  private int blockIndex;

  // Tracks the blockIndex corresponding to the last seeked position so that it
  // can be reset if a new position is seeked.
  private int blockIndexOfPrevPosition;

  public TabletInputStream() {
    blockStreams = new ArrayList<>();
    blockIndex = 0;
  }

  /**
   * For each block in tabletInfo, add a BlockInputStream to blockStreams.
   */
  public static LengthInputStream getFromOmTabletInfo(OmTabletInfo tabletInfo,
                                                   XceiverClientFactory xceiverClientFactory,
                                                   boolean verifyChecksum, Function<OmTabletInfo, OmTabletInfo> retryFunction) {
    List<OmTabletLocationInfo> tabletLocationInfos = tabletInfo
        .getLatestVersionLocations().getBlocksLatestVersionOnly();

    TabletInputStream tabletInputStream = new TabletInputStream();
    tabletInputStream.initialize(tabletInfo, tabletLocationInfos,
        xceiverClientFactory, verifyChecksum, retryFunction);

    return new LengthInputStream(tabletInputStream, tabletInputStream.length);
  }

  public static List<LengthInputStream> getStreamsFromTabletInfo(OmTabletInfo tabletInfo,
      XceiverClientFactory xceiverClientFactory, boolean verifyChecksum,
      Function<OmTabletInfo, OmTabletInfo> retryFunction) {
    List<OmTabletLocationInfo> tabletLocationInfos = tabletInfo
        .getLatestVersionLocations().getBlocksLatestVersionOnly();

    List<LengthInputStream> lengthInputStreams = new ArrayList<>();

    // Iterate through each block info in tabletLocationInfos and assign it the
    // corresponding part in the partsToBlockMap. Also increment each part's
    // length accordingly.
    Map<Integer, List<OmTabletLocationInfo>> partsToBlocksMap = new HashMap<>();
    Map<Integer, Long> partsLengthMap = new HashMap<>();

    for (OmTabletLocationInfo omTabletLocationInfo: tabletLocationInfos) {
      int partNumber = omTabletLocationInfo.getPartNumber();

      if (!partsToBlocksMap.containsKey(partNumber)) {
        partsToBlocksMap.put(partNumber, new ArrayList<>());
        partsLengthMap.put(partNumber, 0L);
      }
      // Add Block to corresponding partNumber in partsToBlocksMap
      partsToBlocksMap.get(partNumber).add(omTabletLocationInfo);
      // Update the part length
      partsLengthMap.put(partNumber,
          partsLengthMap.get(partNumber) + omTabletLocationInfo.getLength());
    }

    // Create a TabletInputStream for each part.
    for (Map.Entry<Integer, List<OmTabletLocationInfo>> entry :
        partsToBlocksMap.entrySet()) {
      TabletInputStream tabletInputStream = new TabletInputStream();
      tabletInputStream.initialize(tabletInfo, entry.getValue(),
          xceiverClientFactory, verifyChecksum, retryFunction);
      lengthInputStreams.add(new LengthInputStream(tabletInputStream,
          partsLengthMap.get(entry.getKey())));
    }

    return lengthInputStreams;
  }

  private synchronized void initialize(OmTabletInfo tabletInfo,
      List<OmTabletLocationInfo> blockInfos,
      XceiverClientFactory xceiverClientFactory,
      boolean verifyChecksum,  Function<OmTabletInfo, OmTabletInfo> retryFunction) {
    this.tablet = tabletInfo.getTabletName();
    this.blockOffsets = new long[blockInfos.size()];
    long tabletLength = 0;
    for (int i = 0; i < blockInfos.size(); i++) {
      OmTabletLocationInfo omTabletLocationInfo = blockInfos.get(i);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Adding stream for accessing {}. The stream will be " +
            "initialized later.", omTabletLocationInfo);
      }

      // We also pass in functional reference which is used to refresh the
      // pipeline info for a given OM Tablet location info.
      addStream(omTabletLocationInfo, xceiverClientFactory,
          verifyChecksum, tabletLocationInfo -> {
            OmTabletInfo newTabletInfo = retryFunction.apply(tabletInfo);
            BlockID blockID = tabletLocationInfo.getBlockID();
            List<OmTabletLocationInfo> collect =
                newTabletInfo.getLatestVersionLocations()
                .getLocationList()
                .stream()
                .filter(l -> l.getBlockID().equals(blockID))
                .collect(Collectors.toList());
            if (CollectionUtils.isNotEmpty(collect)) {
              return collect.get(0).getPipeline();
            } else {
              return null;
            }
          });

      this.blockOffsets[i] = tabletLength;
      tabletLength += omTabletLocationInfo.getLength();
    }
    this.length = tabletLength;
  }

  /**
   * Append another BlockInputStream to the end of the list. Note that the
   * BlockInputStream is only created here and not initialized. The
   * BlockInputStream is initialized when a read operation is performed on
   * the block for the first time.
   */
  private synchronized void addStream(OmTabletLocationInfo blockInfo,
      XceiverClientFactory xceiverClientFactory,
      boolean verifyChecksum,
      Function<OmTabletLocationInfo, Pipeline> refreshPipelineFunction) {
    blockStreams.add(new BlockInputStream(blockInfo.getBlockID(),
        blockInfo.getLength(), blockInfo.getPipeline(), blockInfo.getToken(),
        verifyChecksum, xceiverClientFactory,
        blockID -> refreshPipelineFunction.apply(blockInfo)));
  }

  @VisibleForTesting
  public void addStream(BlockInputStream blockInputStream) {
    blockStreams.add(blockInputStream);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized int read() throws IOException {
    byte[] buf = new byte[1];
    if (read(buf, 0, 1) == EOF) {
      return EOF;
    }
    return Byte.toUnsignedInt(buf[0]);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized int read(byte[] b, int off, int len) throws IOException {
    ByteReaderStrategy strategy = new ByteArrayReader(b, off, len);
    int bufferLen = strategy.getTargetLength();
    if (bufferLen == 0) {
      return 0;
    }
    return readWithStrategy(strategy);
  }

  @Override
  public synchronized int read(ByteBuffer byteBuffer) throws IOException {
    ByteReaderStrategy strategy = new ByteBufferReader(byteBuffer);
    int bufferLen = strategy.getTargetLength();
    if (bufferLen == 0) {
      return 0;
    }
    return readWithStrategy(strategy);
  }

  synchronized int readWithStrategy(ByteReaderStrategy strategy) throws
      IOException {
    Preconditions.checkArgument(strategy != null);
    checkOpen();

    int buffLen = strategy.getTargetLength();
    int totalReadLen = 0;
    while (buffLen > 0) {
      // if we are at the last block and have read the entire block, return
      if (blockStreams.size() == 0 ||
          (blockStreams.size() - 1 <= blockIndex &&
              blockStreams.get(blockIndex)
                  .getRemaining() == 0)) {
        return totalReadLen == 0 ? EOF : totalReadLen;
      }

      // Get the current blockStream and read data from it
      BlockInputStream current = blockStreams.get(blockIndex);
      int numBytesToRead = Math.min(buffLen, (int)current.getRemaining());
      int numBytesRead = strategy.readFromBlock(current, numBytesToRead);
      if (numBytesRead != numBytesToRead) {
        // This implies that there is either data loss or corruption in the
        // chunk entries. Even EOF in the current stream would be covered in
        // this case.
        throw new IOException(String.format("Inconsistent read for blockID=%s "
                + "length=%d numBytesToRead=%d numBytesRead=%d",
            current.getBlockID(), current.getLength(), numBytesToRead,
            numBytesRead));
      }
      totalReadLen += numBytesRead;
      buffLen -= numBytesRead;
      if (current.getRemaining() <= 0 &&
          ((blockIndex + 1) < blockStreams.size())) {
        blockIndex += 1;
      }
    }
    return totalReadLen;
  }

  /**
   * Seeks the TabletInputStream to the specified position. This involves 2 steps:
   *    1. Updating the blockIndex to the blockStream corresponding to the
   *    seeked position.
   *    2. Seeking the corresponding blockStream to the adjusted position.
   *
   * For example, let’s say the block size is 200 bytes and block[0] stores
   * data from indices 0 - 199, block[1] from indices 200 - 399 and so on.
   * Let’s say we seek to position 240. In the first step, the blockIndex
   * would be updated to 1 as indices 200 - 399 reside in blockStream[1]. In
   * the second step, the blockStream[1] would be seeked to position 40 (=
   * 240 - blockOffset[1] (= 200)).
   */
  @Override
  public synchronized void seek(long pos) throws IOException {
    checkOpen();
    if (pos == 0 && length == 0) {
      // It is possible for length and pos to be zero in which case
      // seek should return instead of throwing exception
      return;
    }
    if (pos < 0 || pos > length) {
      throw new EOFException(
          "EOF encountered at pos: " + pos + " for tablet: " + tablet);
    }

    // 1. Update the blockIndex
    if (blockIndex >= blockStreams.size()) {
      blockIndex = Arrays.binarySearch(blockOffsets, pos);
    } else if (pos < blockOffsets[blockIndex]) {
      blockIndex =
          Arrays.binarySearch(blockOffsets, 0, blockIndex, pos);
    } else if (pos >= blockOffsets[blockIndex] + blockStreams
        .get(blockIndex).getLength()) {
      blockIndex = Arrays
          .binarySearch(blockOffsets, blockIndex + 1,
              blockStreams.size(), pos);
    }
    if (blockIndex < 0) {
      // Binary search returns -insertionPoint - 1  if element is not present
      // in the array. insertionPoint is the point at which element would be
      // inserted in the sorted array. We need to adjust the blockIndex
      // accordingly so that blockIndex = insertionPoint - 1
      blockIndex = -blockIndex - 2;
    }

    // Reset the previous blockStream's position
    blockStreams.get(blockIndexOfPrevPosition).resetPosition();

    // Reset all the blockStreams above the blockIndex. We do this to reset
    // any previous reads which might have updated the blockPosition and
    // chunkIndex.
    for (int index =  blockIndex + 1; index < blockStreams.size(); index++) {
      blockStreams.get(index).seek(0);
    }
    // 2. Seek the blockStream to the adjusted position
    blockStreams.get(blockIndex).seek(pos - blockOffsets[blockIndex]);
    blockIndexOfPrevPosition = blockIndex;
  }

  @Override
  public synchronized long getPos() throws IOException {
    return length == 0 ? 0 : blockOffsets[blockIndex] +
        blockStreams.get(blockIndex).getPos();
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }

  @Override
  public int available() throws IOException {
    checkOpen();
    long remaining = length - getPos();
    return remaining <= Integer.MAX_VALUE ? (int) remaining : Integer.MAX_VALUE;
  }

  @Override
  public void close() throws IOException {
    closed = true;
    for (BlockInputStream blockStream : blockStreams) {
      blockStream.close();
    }
  }

  /**
   * Verify that the input stream is open. Non blocking; this gives
   * the last state of the volatile {@link #closed} field.
   * @throws IOException if the connection is closed.
   */
  private void checkOpen() throws IOException {
    if (closed) {
      throw new IOException(
          ": " + FSExceptionMessages.STREAM_IS_CLOSED + " Tablet: " + tablet);
    }
  }

  @VisibleForTesting
  public synchronized int getCurrentStreamIndex() {
    return blockIndex;
  }

  @VisibleForTesting
  public long getRemainingOfIndex(int index) throws IOException {
    return blockStreams.get(index).getRemaining();
  }

  @Override
  public long skip(long n) throws IOException {
    if (n <= 0) {
      return 0;
    }

    long toSkip = Math.min(n, length - getPos());
    seek(getPos() + toSkip);
    return toSkip;
  }

  @Override
  public void unbuffer() {
    for (BlockInputStream is : blockStreams) {
      is.unbuffer();
    }
  }

  @VisibleForTesting
  public List<BlockInputStream> getBlockStreams() {
    return blockStreams;
  }
}
