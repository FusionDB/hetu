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
package org.apache.hadoop.hetu.client;

import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.BlockData;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.DatanodeBlockID;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChecksumData;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * State represents persisted data of one specific datanode.
 */
public class MockDatanodeStorage {
  static final Logger LOG = LoggerFactory.getLogger(MockDatanodeStorage.class);

  private final Map<DatanodeBlockID, BlockData> blocks = new HashedMap();

  private final Map<String, ChunkInfo> chunks = new HashMap<>();

  private final Map<String, List<ByteString>> data = new HashMap<>();
  private int i = 0;

  public void putBlock(DatanodeBlockID blockID, BlockData blockData) {
    // append mode: update block length, Get the latest chunkInfo
    List<ChunkInfo> chunkInfoList = blockData.getChunksList();
    List<ChunkInfo> targetChunkInfoList = chunkInfoList.stream()
            .map(chunkInfo ->  chunks.get(createKey(blockID, chunkInfo)))
            .collect(Collectors.toList());

    BlockData targetBlockData = BlockData.newBuilder()
            .setBlockID(blockID)
            .addAllMetadata(blockData.getMetadataList())
            .addAllChunks(targetChunkInfoList)
            .setFlags(blockData.getFlags())
            .build();
    blocks.put(blockID, targetBlockData);
    i++;
    LOG.info("Exec batch [{}] of putBlock", i);
  }

  public BlockData getBlock(DatanodeBlockID blockID) {
    return blocks.get(blockID);
  }

  public void writeChunk(
      DatanodeBlockID blockID,
      ChunkInfo chunkInfo, ByteString bytes) {
    List<ByteString> byteStringList = data.get(createKey(blockID, chunkInfo));
    if (null == byteStringList) {
      byteStringList = new ArrayList<>();
      chunks.put(createKey(blockID, chunkInfo), chunkInfo);
    } else {
      // append mode: update chunk length
      long newLength = chunkInfo.getLen();
      ChunkInfo currentChunkInfo = chunks.get(createKey(blockID, chunkInfo));

      ChunkInfo.Builder targetChunkInfo = ChunkInfo.newBuilder()
              .setChunkName(currentChunkInfo.getChunkName())
              .setLen(currentChunkInfo.getLen() + newLength)
              .addAllMetadata(currentChunkInfo.getMetadataList())
              .setOffset(currentChunkInfo.getOffset());

      // TODO: recount check sum data
      targetChunkInfo.setChecksumData(currentChunkInfo.getChecksumData());

      chunks.put(createKey(blockID, chunkInfo), targetChunkInfo.build());
    }
    byteStringList.add(bytes);
    data.put(createKey(blockID, chunkInfo), byteStringList);
  }

  public ChunkInfo readChunkInfo(
      DatanodeBlockID blockID,
      ChunkInfo chunkInfo) {
    return chunks.get(createKey(blockID, chunkInfo));
  }

  public List<ByteString> readChunkData(
      DatanodeBlockID blockID,
      ChunkInfo chunkInfo) {
    return data.get(createKey(blockID, chunkInfo));
  }

  private String createKey(DatanodeBlockID blockId, ChunkInfo chunkInfo) {
    return blockId.getContainerID() + "_" + blockId.getLocalID() + "_"
        + chunkInfo.getChunkName() + "_" + chunkInfo.getOffset();
  }

}
