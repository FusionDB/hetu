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

package org.apache.hadoop.ozone.om.request;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.google.common.base.Optional;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.hetu.hm.helpers.OmDatabaseArgs;
import org.apache.hadoop.hetu.photon.meta.common.ColumnKey;
import org.apache.hadoop.hetu.photon.meta.table.ColumnSchema;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.*;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
        .SetDatabasePropertyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .MultipartUploadAbortRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .MultipartCommitUploadPartRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .MultipartUploadCompleteRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .MultipartInfoInitiateRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .SetVolumePropertyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .AddAclRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .RemoveAclRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .SetAclRequest;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType;
import org.apache.hadoop.ozone.security.acl.OzoneObj.StoreType;

import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.jetbrains.annotations.NotNull;

import static java.util.stream.Collectors.toList;

/**
 * Helper class to test OMClientRequest classes.
 */
public final class TestOMRequestUtils {

  private TestOMRequestUtils() {
    //Do nothing
  }

  /**
   * Add's volume and bucket creation entries to OM DB.
   * @param volumeName
   * @param bucketName
   * @param omMetadataManager
   * @throws Exception
   */
  public static void addVolumeAndBucketToDB(String volumeName,
      String bucketName, OMMetadataManager omMetadataManager) throws Exception {

    addVolumeToDB(volumeName, omMetadataManager);

    addBucketToDB(volumeName, bucketName, omMetadataManager);
  }


  /**
   * Add's database and table creation entries to OM DB.
   * @param databaseName
   * @param tableName
   * @param omMetadataManager
   * @throws Exception
   */
  public static void addDatabaseAndTableToDB(String databaseName,
                                            String tableName, OMMetadataManager omMetadataManager) throws Exception {

    addDatabaseToDB(databaseName, omMetadataManager);

    addMetaTableToDB(databaseName, tableName, omMetadataManager);
  }

  @SuppressWarnings("parameterNumber")
  public static void addKeyToTableAndCache(String volumeName, String bucketName,
      String keyName, long clientID, HddsProtos.ReplicationType replicationType,
      HddsProtos.ReplicationFactor replicationFactor, long trxnLogIndex,
      OMMetadataManager omMetadataManager) throws Exception {
    addKeyToTable(false, true, volumeName, bucketName, keyName, clientID,
        replicationType, replicationFactor, trxnLogIndex, omMetadataManager);
  }

  /**
   * Add key entry to KeyTable. if openKeyTable flag is true, add's entries
   * to openKeyTable, else add's it to keyTable.
   * @param openKeyTable
   * @param volumeName
   * @param bucketName
   * @param keyName
   * @param clientID
   * @param replicationType
   * @param replicationFactor
   * @param omMetadataManager
   * @param locationList
   * @throws Exception
   */
  @SuppressWarnings("parameterNumber")
  public static void addKeyToTable(boolean openKeyTable, String volumeName,
      String bucketName, String keyName, long clientID,
      HddsProtos.ReplicationType replicationType,
      HddsProtos.ReplicationFactor replicationFactor,
      OMMetadataManager omMetadataManager,
      List<OmKeyLocationInfo> locationList) throws Exception {
    addKeyToTable(openKeyTable, false, volumeName, bucketName, keyName,
        clientID, replicationType, replicationFactor, 0L, omMetadataManager,
        locationList);
  }


  /**
   * Add key entry to KeyTable. if openKeyTable flag is true, add's entries
   * to openKeyTable, else add's it to keyTable.
   * @param openKeyTable
   * @param volumeName
   * @param bucketName
   * @param keyName
   * @param clientID
   * @param replicationType
   * @param replicationFactor
   * @param omMetadataManager
   * @throws Exception
   */
  @SuppressWarnings("parameterNumber")
  public static void addKeyToTable(boolean openKeyTable, String volumeName,
      String bucketName, String keyName, long clientID,
      HddsProtos.ReplicationType replicationType,
      HddsProtos.ReplicationFactor replicationFactor,
      OMMetadataManager omMetadataManager) throws Exception {
    addKeyToTable(openKeyTable, false, volumeName, bucketName, keyName,
        clientID, replicationType, replicationFactor, 0L, omMetadataManager);
  }

  /**
   * Add tablet entry to tabletTable. if openTabletTable flag is true, add's entries
   * to openTabletTable, else add's it to tabletTable.
   * @param openTabletTable
   * @param databaseName
   * @param tableName
   * @param partitionName
   * @param tabletName
   * @param clientID
   * @param replicationType
   * @param replicationFactor
   * @param omMetadataManager
   * @throws Exception
   */
  @SuppressWarnings("parameterNumber")
  public static void addTabletToTable(boolean openTabletTable, String databaseName,
                                      String tableName, String partitionName, String tabletName,
                                      long clientID, HddsProtos.ReplicationType replicationType,
                                      HddsProtos.ReplicationFactor replicationFactor,
                                      OMMetadataManager omMetadataManager) throws Exception {
    addTabletToTable(openTabletTable, false, databaseName, tableName, partitionName, tabletName,
            clientID, replicationType, replicationFactor, 0L, omMetadataManager);
  }

  /**
   * Add key entry to tabletTable. if openTabletTable flag is true, add's entries
   * to openTabletTable, else add's it to tabletTable.
   * @throws Exception
   */
  @SuppressWarnings("parameternumber")
  public static void addKeyToTable(boolean openKeyTable, boolean addToCache,
      String volumeName, String bucketName, String keyName, long clientID,
      HddsProtos.ReplicationType replicationType,
      HddsProtos.ReplicationFactor replicationFactor, long trxnLogIndex,
      OMMetadataManager omMetadataManager,
      List<OmKeyLocationInfo> locationList) throws Exception {

    OmKeyInfo omKeyInfo = createOmKeyInfo(volumeName, bucketName, keyName,
        replicationType, replicationFactor, trxnLogIndex);
    omKeyInfo.appendNewBlocks(locationList, false);

    addKeyToTable(openKeyTable, addToCache, omKeyInfo, clientID, trxnLogIndex,
            omMetadataManager);
  }

  /**
   * Add tablet entry to tabletTable. if openTabletTable flag is true, add's entries
   * to openTabletTable, else add's it to tabletTable.
   * @throws Exception
   */
  @SuppressWarnings("parameternumber")
  public static void addTabletToTable(boolean openTabletTable, boolean addToCache,
                                   String databaseName, String tableName, String partitionName,
                                   String tabletName, long clientID, HddsProtos.ReplicationType replicationType,
                                   HddsProtos.ReplicationFactor replicationFactor, long trxnLogIndex,
                                   OMMetadataManager omMetadataManager,
                                   List<OmTabletLocationInfo> locationList) throws Exception {

    OmTabletInfo omTabletInfo = createOmTabletInfo(databaseName, tableName, partitionName, tabletName,
            replicationType, replicationFactor, trxnLogIndex);
    omTabletInfo.appendNewBlocks(locationList, false);

    addTabletToTable(openTabletTable, addToCache, omTabletInfo, clientID, trxnLogIndex,
            omMetadataManager);
  }

  /**
   * Add key entry to KeyTable. if openKeyTable flag is true, add's entries
   * to openKeyTable, else add's it to keyTable.
   * @throws Exception
   */
  @SuppressWarnings("parameternumber")
  public static void addKeyToTable(boolean openKeyTable, boolean addToCache,
      String volumeName, String bucketName, String keyName, long clientID,
      HddsProtos.ReplicationType replicationType,
      HddsProtos.ReplicationFactor replicationFactor, long trxnLogIndex,
      OMMetadataManager omMetadataManager) throws Exception {

    OmKeyInfo omKeyInfo = createOmKeyInfo(volumeName, bucketName, keyName,
        replicationType, replicationFactor, trxnLogIndex);

    addKeyToTable(openKeyTable, addToCache, omKeyInfo, clientID, trxnLogIndex,
        omMetadataManager);
  }

  /**
   * Add tablet entry to tabletTable. if openTabletTable flag is true, add's entries
   * to openTabletTable, else add's it to tabletTable.
   * @throws Exception
   */
  @SuppressWarnings("parameternumber")
  public static void addTabletToTable(boolean openTabletTable, boolean addToCache,
                                   String databaseName, String tableName, String partitionName,
                                   String tabletName, long clientID,
                                   HddsProtos.ReplicationType replicationType,
                                   HddsProtos.ReplicationFactor replicationFactor, long trxnLogIndex,
                                   OMMetadataManager omMetadataManager) throws Exception {

    OmTabletInfo omTabletInfo = createOmTabletInfo(databaseName, tableName, partitionName,
            tabletName, replicationType, replicationFactor, trxnLogIndex);

    addTabletToTable(openTabletTable, addToCache, omTabletInfo, clientID, trxnLogIndex,
            omMetadataManager);
  }

  /**
   * Add key entry to KeyTable. if openKeyTable flag is true, add's entries
   * to openKeyTable, else add's it to keyTable.
   * @throws Exception
   */
  public static void addKeyToTable(boolean openKeyTable, boolean addToCache,
                                   OmKeyInfo omKeyInfo,  long clientID,
                                   long trxnLogIndex,
                                   OMMetadataManager omMetadataManager)
          throws Exception {

    String volumeName = omKeyInfo.getVolumeName();
    String bucketName = omKeyInfo.getBucketName();
    String keyName = omKeyInfo.getKeyName();

    if (openKeyTable) {
      String ozoneKey = omMetadataManager.getOpenKey(volumeName, bucketName,
          keyName, clientID);
      if (addToCache) {
        omMetadataManager.getOpenKeyTable().addCacheEntry(
            new CacheKey<>(ozoneKey),
            new CacheValue<>(Optional.of(omKeyInfo), trxnLogIndex));
      }
      omMetadataManager.getOpenKeyTable().put(ozoneKey, omKeyInfo);
    } else {
      String ozoneKey = omMetadataManager.getOzoneKey(volumeName, bucketName,
          keyName);
      if (addToCache) {
        omMetadataManager.getKeyTable().addCacheEntry(new CacheKey<>(ozoneKey),
            new CacheValue<>(Optional.of(omKeyInfo), trxnLogIndex));
      }
      omMetadataManager.getKeyTable().put(ozoneKey, omKeyInfo);
    }
  }

  /**
   * Add tablet entry to tabletTable. if openTabletTable flag is true, add's entries
   * to openTabletTable, else add's it to tabletTable.
   * @throws Exception
   */
  public static void addTabletToTable(boolean openTabletTable, boolean addToCache,
                                   OmTabletInfo omTabletInfo,  long clientID,
                                   long trxnLogIndex,
                                   OMMetadataManager omMetadataManager)
          throws Exception {

    String databaseName = omTabletInfo.getDatabaseName();
    String tableName = omTabletInfo.getTableName();
    String partitionName = omTabletInfo.getPartitionName();
    String tabletName = omTabletInfo.getTabletName();

    if (openTabletTable) {
      String ozoneTablet = omMetadataManager.getOpenTablet(databaseName, tableName,
              partitionName, tabletName, clientID);
      if (addToCache) {
        omMetadataManager.getOpenTabletTable().addCacheEntry(
                new CacheKey<>(ozoneTablet),
                new CacheValue<>(Optional.of(omTabletInfo), trxnLogIndex));
      }
      omMetadataManager.getOpenTabletTable().put(ozoneTablet, omTabletInfo);
    } else {
      String ozoneTablet = omMetadataManager.getOzoneTablet(databaseName, tableName,
              partitionName, tabletName);
      if (addToCache) {
        omMetadataManager.getTabletTable().addCacheEntry(new CacheKey<>(ozoneTablet),
                new CacheValue<>(Optional.of(omTabletInfo), trxnLogIndex));
      }
      omMetadataManager.getTabletTable().put(ozoneTablet, omTabletInfo);
    }
  }

  /**
   * Add key entry to key table cache.
   * @param volumeName
   * @param bucketName
   * @param keyName
   * @param replicationType
   * @param replicationFactor
   * @param omMetadataManager
   */
  @SuppressWarnings("parameterNumber")
  public static void addKeyToTableCache(String volumeName,
      String bucketName,
      String keyName,
      HddsProtos.ReplicationType replicationType,
      HddsProtos.ReplicationFactor replicationFactor,
      OMMetadataManager omMetadataManager) {

    OmKeyInfo omKeyInfo = createOmKeyInfo(volumeName, bucketName, keyName,
        replicationType, replicationFactor);

    omMetadataManager.getKeyTable().addCacheEntry(
        new CacheKey<>(omMetadataManager.getOzoneKey(volumeName, bucketName,
            keyName)), new CacheValue<>(Optional.of(omKeyInfo), 1L));
  }

  /**
   * Add tablet entry to tablet table cache.
   * @param databaseName
   * @param tableName
   * @param partitionName
   * @param tabletName
   * @param replicationType
   * @param replicationFactor
   * @param omMetadataManager
   */
  @SuppressWarnings("parameterNumber")
  public static void addTabletToTableCache(String databaseName,
                                        String tableName,
                                        String partitionName,
                                        String tabletName,
                                        HddsProtos.ReplicationType replicationType,
                                        HddsProtos.ReplicationFactor replicationFactor,
                                        OMMetadataManager omMetadataManager) {

    OmTabletInfo omTabletInfo = createOmTabletInfo(databaseName, tableName, partitionName, tabletName,
            replicationType, replicationFactor);

    omMetadataManager.getTabletTable().addCacheEntry(
            new CacheKey<>(omMetadataManager.getOzoneTablet(databaseName, tableName, partitionName,
                    tabletName)), new CacheValue<>(Optional.of(omTabletInfo), 1L));
  }

  /**
   * Adds one block to {@code keyInfo} with the provided size and offset.
   */
  public static void addKeyLocationInfo(
      OmKeyInfo keyInfo, long offset, long keyLength) throws IOException {

    Pipeline pipeline = Pipeline.newBuilder()
        .setState(Pipeline.PipelineState.OPEN)
        .setId(PipelineID.randomId())
        .setReplicationConfig(ReplicationConfig
            .fromTypeAndFactor(keyInfo.getType(), keyInfo.getFactor()))
        .setNodes(new ArrayList<>())
        .build();

    OmKeyLocationInfo locationInfo = new OmKeyLocationInfo.Builder()
          .setBlockID(new BlockID(100L, 1000L))
          .setOffset(offset)
          .setLength(keyLength)
          .setPipeline(pipeline)
          .build();

    keyInfo.appendNewBlocks(Collections.singletonList(locationInfo), false);
  }

  /**
   * Adds one block to {@code tabletInfo} with the provided size and offset.
   */
  public static void addTabletLocationInfo(
          OmTabletInfo tabletInfo, long offset, long keyLength) throws IOException {

    Pipeline pipeline = Pipeline.newBuilder()
            .setState(Pipeline.PipelineState.OPEN)
            .setId(PipelineID.randomId())
            .setReplicationConfig(ReplicationConfig
                    .fromTypeAndFactor(tabletInfo.getType(), tabletInfo.getFactor()))
            .setNodes(new ArrayList<>())
            .build();

    OmTabletLocationInfo locationInfo = new OmTabletLocationInfo.Builder()
            .setBlockID(new BlockID(100L, 1000L))
            .setOffset(offset)
            .setLength(keyLength)
            .setPipeline(pipeline)
            .build();

    tabletInfo.appendNewBlocks(Collections.singletonList(locationInfo), false);
  }

  /**
   * Create OmKeyInfo.
   */
  public static OmKeyInfo createOmKeyInfo(String volumeName, String bucketName,
      String keyName, HddsProtos.ReplicationType replicationType,
      HddsProtos.ReplicationFactor replicationFactor) {
    return createOmKeyInfo(volumeName, bucketName, keyName, replicationType,
        replicationFactor, 0L);
  }

  /**
   * Create OmTabletInfo.
   */
  public static OmTabletInfo createOmTabletInfo(String databaseName, String tableName,
                                          String partitionName, String tabletName, HddsProtos.ReplicationType replicationType,
                                          HddsProtos.ReplicationFactor replicationFactor) {
    return createOmTabletInfo(databaseName, tableName, partitionName, tabletName, replicationType,
            replicationFactor, 0L);
  }

  /**
   * Create OmKeyInfo.
   */
  public static OmKeyInfo createOmKeyInfo(String volumeName, String bucketName,
      String keyName, HddsProtos.ReplicationType replicationType,
      HddsProtos.ReplicationFactor replicationFactor, long objectID) {
    return createOmKeyInfo(volumeName, bucketName, keyName, replicationType,
            replicationFactor, objectID, Time.now());
  }

  /**
   * Create OmTabletInfo.
   */
  public static OmTabletInfo createOmTabletInfo(String databaseName, String tableName, String partitionName,
                                          String tabletName, HddsProtos.ReplicationType replicationType,
                                          HddsProtos.ReplicationFactor replicationFactor, long objectID) {
    return createOmTabletInfo(databaseName, tableName, partitionName, tabletName, replicationType,
            replicationFactor, objectID, Time.now());
  }

  /**
   * Create OmKeyInfo.
   */
  public static OmKeyInfo createOmKeyInfo(String volumeName, String bucketName,
      String keyName, HddsProtos.ReplicationType replicationType,
      HddsProtos.ReplicationFactor replicationFactor, long objectID,
      long creationTime) {
    return new OmKeyInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setOmKeyLocationInfos(Collections.singletonList(
            new OmKeyLocationInfoGroup(0, new ArrayList<>())))
        .setCreationTime(creationTime)
        .setModificationTime(Time.now())
        .setDataSize(1000L)
        .setReplicationType(replicationType)
        .setReplicationFactor(replicationFactor)
        .setObjectID(objectID)
        .setUpdateID(objectID)
        .build();
  }

  /**
   * Create OmTetInfo.
   */
  public static OmTabletInfo createOmTabletInfo(String databaseName, String tableName, String partitionName,
                                          String tabletName, HddsProtos.ReplicationType replicationType,
                                          HddsProtos.ReplicationFactor replicationFactor, long objectID,
                                          long creationTime) {
    return new OmTabletInfo.Builder()
            .setDatabaseName(databaseName)
            .setTableName(tableName)
            .setPartitionName(partitionName)
            .setTabletName(tabletName)
            .setOmTabletLocationInfos(Collections.singletonList(
                    new OmTabletLocationInfoGroup(0, new ArrayList<>())))
            .setCreationTime(creationTime)
            .setModificationTime(Time.now())
            .setDataSize(1000L)
            .setReplicationType(replicationType)
            .setReplicationFactor(replicationFactor)
            .setObjectID(objectID)
            .setUpdateID(objectID)
            .addAllMetadata(getMetadataMap())
            .build();
  }

  private static Map<String, String> getMetadataMap() {
    Map<String, String> map = new HashMap<>();
    map.put("k1", "v1");
    map.put("k2", "v2");
    map.put(OzoneConsts.GDPR_FLAG, "false");
    return map;
  }

  /**
   * Add volume creation entry to OM DB.
   * @param volumeName
   * @param omMetadataManager
   * @throws Exception
   */
  public static void addVolumeToDB(String volumeName,
      OMMetadataManager omMetadataManager) throws Exception {
    addVolumeToDB(volumeName, UUID.randomUUID().toString(), omMetadataManager);
  }

  /**
   * Add volume creation entry to OM DB.
   * @param volumeName
   * @param omMetadataManager
   * @param quotaInBytes
   * @throws Exception
   */
  public static void addVolumeToDB(String volumeName,
      OMMetadataManager omMetadataManager, long quotaInBytes) throws Exception {
    OmVolumeArgs omVolumeArgs =
        OmVolumeArgs.newBuilder().setCreationTime(Time.now())
            .setVolume(volumeName).setAdminName(volumeName)
            .setOwnerName(volumeName).setQuotaInBytes(quotaInBytes)
            .setQuotaInNamespace(10000L).build();
    omMetadataManager.getVolumeTable().put(
        omMetadataManager.getVolumeKey(volumeName), omVolumeArgs);

    // Add to cache.
    omMetadataManager.getVolumeTable().addCacheEntry(
        new CacheKey<>(omMetadataManager.getVolumeKey(volumeName)),
        new CacheValue<>(Optional.of(omVolumeArgs), 1L));
  }

  /**
   * Add volume creation entry to OM DB.
   * @param volumeName
   * @param ownerName
   * @param omMetadataManager
   * @throws Exception
   */
  public static void addVolumeToDB(String volumeName, String ownerName,
      OMMetadataManager omMetadataManager) throws Exception {
    OmVolumeArgs omVolumeArgs =
        OmVolumeArgs.newBuilder().setCreationTime(Time.now())
            .setVolume(volumeName).setAdminName(ownerName)
            .setOwnerName(ownerName).setQuotaInBytes(Long.MAX_VALUE)
            .setQuotaInNamespace(10000L).build();
    omMetadataManager.getVolumeTable().put(
        omMetadataManager.getVolumeKey(volumeName), omVolumeArgs);

    // Add to cache.
    omMetadataManager.getVolumeTable().addCacheEntry(
        new CacheKey<>(omMetadataManager.getVolumeKey(volumeName)),
            new CacheValue<>(Optional.of(omVolumeArgs), 1L));
  }

  /**
   * Add bucket creation entry to OM DB.
   * @param volumeName
   * @param bucketName
   * @param omMetadataManager
   * @throws Exception
   */
  public static void addBucketToDB(String volumeName, String bucketName,
      OMMetadataManager omMetadataManager) throws Exception {

    OmBucketInfo omBucketInfo =
        OmBucketInfo.newBuilder().setVolumeName(volumeName)
            .setBucketName(bucketName).setCreationTime(Time.now()).build();

    // Add to cache.
    omMetadataManager.getBucketTable().addCacheEntry(
        new CacheKey<>(omMetadataManager.getBucketKey(volumeName, bucketName)),
        new CacheValue<>(Optional.of(omBucketInfo), 1L));
  }

  /**
   * Add table creation entry to OM DB.
   * @param databaseName
   * @param tableName
   * @param omMetadataManager
   * @throws Exception
   */
  public static void addMetaTableToDB(String databaseName, String tableName,
                                   OMMetadataManager omMetadataManager) throws Exception {

    ColumnSchema col1 = new ColumnSchema(
            "city",
            "varchar(4)",
            "varcher",
            0,
            "",
            "",
            "用户",
            true);

    ColumnSchema col2 = new ColumnSchema(
            "id",
            "Long",
            "Long",
            1,
            "",
            "",
            "ID",
            true);

    OzoneManagerProtocolProtos.TableInfo.PartitionsProto partitionsProto = getPartitionsProto();

    OmTableInfo omTableInfo = OmTableInfo.newBuilder()
            .setDatabaseName(databaseName)
            .setTableName(tableName)
            .setColumns(Arrays.asList(col1, col2))
            .setPartitions(partitionsProto)
            .setColumnKey(ColumnKey.fromProtobuf(getColumnKeyProto()))
            .setCreationTime(Time.now())
            .setUsedInBytes(0L).build();

    // Add to cache.
    omMetadataManager.getMetaTable().addCacheEntry(
            new CacheKey<>(omMetadataManager.getMetaTableKey(databaseName, tableName)),
            new CacheValue<>(Optional.of(omTableInfo), 1L));
  }

  @NotNull
  public static OzoneManagerProtocolProtos.TableInfo.PartitionsProto getPartitionsProto() {
    return OzoneManagerProtocolProtos.TableInfo.PartitionsProto.newBuilder()
            .addAllFields(Arrays.asList("city"))
            .setPartitionType(OzoneManagerProtocolProtos.TableInfo.Type.RANGE)
            .build();
  }

  /**
   * Add bucket creation entry to OM DB.
   * @param volumeName
   * @param bucketName
   * @param omMetadataManager
   * @param quotaInBytes
   * @throws Exception
   */
  public static void addBucketToDB(String volumeName, String bucketName,
      OMMetadataManager omMetadataManager, long quotaInBytes) throws Exception {

    OmBucketInfo omBucketInfo =
        OmBucketInfo.newBuilder().setVolumeName(volumeName)
            .setBucketName(bucketName).setCreationTime(Time.now())
            .setQuotaInBytes(quotaInBytes).build();

    // Add to cache.
    omMetadataManager.getBucketTable().addCacheEntry(
        new CacheKey<>(omMetadataManager.getBucketKey(volumeName, bucketName)),
        new CacheValue<>(Optional.of(omBucketInfo), 1L));
  }

  /**
   * Add MetaTable creation entry to OM DB.
   * @param databaseName
   * @param tableName
   * @param omMetadataManager
   * @param usedCapacityInBytes
   * @throws Exception
   */
  public static void addMetaTableToDB(String databaseName, String tableName,
                                   OMMetadataManager omMetadataManager, long usedCapacityInBytes) throws Exception {

    ColumnSchema col1 = new ColumnSchema(
            "city",
            "varchar(4)",
            "varcher",
            0,
            "",
            "",
            "用户",
            true);

    ColumnSchema col2 = new ColumnSchema(
            "id",
            "Long",
            "Long",
            1,
            "",
            "",
            "ID",
            true);

    OzoneManagerProtocolProtos.TableInfo.PartitionsProto partitionsProto = getPartitionsProto();

    OmTableInfo omTableInfo = OmTableInfo.newBuilder()
            .setDatabaseName(databaseName)
            .setTableName(tableName)
            .setColumns(Arrays.asList(col1, col2))
            .setColumnKey(ColumnKey.fromProtobuf(getColumnKeyProto()))
            .setPartitions(partitionsProto)
            .setCreationTime(Time.now())
            .setDistributedKey(getDistributedKeyProto())
            .setUsedInBytes(usedCapacityInBytes).build();

    // Add to cache.
    omMetadataManager.getMetaTable().addCacheEntry(
            new CacheKey<>(omMetadataManager.getMetaTableKey(databaseName, tableName)),
            new CacheValue<>(Optional.of(omTableInfo), 1L));
  }

  public static void addPartitionToDB(String databaseName, String tableName,
                                      String partitionName, OMMetadataManager omMetadataManager) {
    OmPartitionInfo omPartitionInfo = OmPartitionInfo.newBuilder()
            .setStorageType(StorageType.DISK)
            .setDatabaseName(databaseName)
            .setTableName(tableName)
            .setPartitionName(partitionName)
            .setSizeInBytes(0L)
            .setBuckets(10)
            .setRows(0)
            .setIsVersionEnabled(false)
            .setUpdateID(1)
            .setObjectID(1)
            .setCreationTime(System.currentTimeMillis())
            .setModificationTime(System.currentTimeMillis())
            .setPartitionValue("20100120")
            .build();

    // Add to cache
    omMetadataManager.getPartitionTable().addCacheEntry(
            new CacheKey<>(omMetadataManager.getPartitionKey(databaseName, tableName, partitionName)),
            new CacheValue<>(Optional.of(omPartitionInfo), 1L));
  }

  public static OzoneManagerProtocolProtos.OMRequest createBucketRequest(
      String bucketName, String volumeName, boolean isVersionEnabled,
      OzoneManagerProtocolProtos.StorageTypeProto storageTypeProto) {
    OzoneManagerProtocolProtos.BucketInfo bucketInfo =
        OzoneManagerProtocolProtos.BucketInfo.newBuilder()
            .setBucketName(bucketName)
            .setVolumeName(volumeName)
            .setIsVersionEnabled(isVersionEnabled)
            .setStorageType(storageTypeProto)
            .addAllMetadata(getMetadataList()).build();
    OzoneManagerProtocolProtos.CreateBucketRequest.Builder req =
        OzoneManagerProtocolProtos.CreateBucketRequest.newBuilder();
    req.setBucketInfo(bucketInfo);
    return OzoneManagerProtocolProtos.OMRequest.newBuilder()
        .setCreateBucketRequest(req)
        .setCmdType(OzoneManagerProtocolProtos.Type.CreateBucket)
        .setClientId(UUID.randomUUID().toString()).build();
  }

  public static OzoneManagerProtocolProtos.OMRequest createTableRequest(
          String tableName, String databaseName, boolean isVersionEnabled,
          OzoneManagerProtocolProtos.StorageTypeProto storageTypeProto) {

    List<OzoneManagerProtocolProtos.ColumnSchemaProto> columnSchemaProtos = getColumnSchemaProtos();

    OzoneManagerProtocolProtos.TableInfo.PartitionsProto partitionsProto = getPartitionsProto();

    OzoneManagerProtocolProtos.TableInfo.ColumnKeyProto columnKeyProto = getColumnKeyProto();

    OzoneManagerProtocolProtos.TableInfo.DistributedKeyProto distributedKeyProto = getDistributedKeyProto();

    OzoneManagerProtocolProtos.TableInfo tableInfo =
            OzoneManagerProtocolProtos.TableInfo.newBuilder()
                    .setTableName(tableName)
                    .setDatabaseName(databaseName)
                    .setIsVersionEnabled(isVersionEnabled)
                    .setStorageType(storageTypeProto)
                    .setStorageEngine(OzoneManagerProtocolProtos.TableInfo.StorageEngineProto.LSTORE)
                    .setNumReplicas(3)
                    .setPartitions(partitionsProto)
                    .addAllColumns(columnSchemaProtos)
                    .setColumnKey(columnKeyProto)
                    .setDistributedKey(distributedKeyProto)
                    .addAllMetadata(getMetadataList()).build();
    OzoneManagerProtocolProtos.CreateTableRequest.Builder req =
            OzoneManagerProtocolProtos.CreateTableRequest.newBuilder();
    req.setTableInfo(tableInfo);
    return OzoneManagerProtocolProtos.OMRequest.newBuilder()
            .setCreateTableRequest(req)
            .setCmdType(OzoneManagerProtocolProtos.Type.CreateTable)
            .setClientId(UUID.randomUUID().toString()).build();
  }

  @NotNull
  public static OzoneManagerProtocolProtos.TableInfo.DistributedKeyProto getDistributedKeyProto() {
    return OzoneManagerProtocolProtos.TableInfo.DistributedKeyProto
            .newBuilder()
            .setDistributedKeyType(OzoneManagerProtocolProtos.TableInfo.Type.HASH)
            .setBuckets(8)
            .addAllFields(Arrays.asList("id"))
            .build();
  }

  @NotNull
  public static OzoneManagerProtocolProtos.TableInfo.ColumnKeyProto getColumnKeyProto() {
    return OzoneManagerProtocolProtos.TableInfo.ColumnKeyProto
            .newBuilder()
            .setColumnKeyType(OzoneManagerProtocolProtos.TableInfo.ColumnKeyTypeProto.PRIMARY_KEY)
            .addAllFields(Arrays.asList("id"))
            .build();
  }

  public static OzoneManagerProtocolProtos.OMRequest createPartitionRequest(
          String tableName, String databaseName, String partitionName, boolean isVersionEnabled,
          OzoneManagerProtocolProtos.StorageTypeProto storageTypeProto) {

    OzoneManagerProtocolProtos.PartitionInfo partitionInfo =
            OzoneManagerProtocolProtos.PartitionInfo.newBuilder()
            .setDatabaseName(databaseName)
            .setTableName(tableName)
            .setIsVersionEnabled(isVersionEnabled)
            .setPartitionName(partitionName)
            .setPartitionValue("201010")
            .setStorageType(storageTypeProto)
            .setBuckets(8)
            .setRows(0)
            .setSizeInBytes(0L)
            .addAllMetadata(getMetadataList())
            .build();

    OzoneManagerProtocolProtos.CreatePartitionRequest.Builder req =
            OzoneManagerProtocolProtos.CreatePartitionRequest.newBuilder();
    req.setPartitionInfo(partitionInfo);
    return OzoneManagerProtocolProtos.OMRequest.newBuilder()
            .setCreatePartitionRequest(req)
            .setCmdType(OzoneManagerProtocolProtos.Type.CreatePartition)
            .setClientId(UUID.randomUUID().toString()).build();
  }

  @NotNull
  public static List<OzoneManagerProtocolProtos.ColumnSchemaProto> getColumnSchemaProtos() {
    List<ColumnSchema> columnSchemaList = getColumnSchemas();
    return columnSchemaList.stream()
            .map(proto -> ColumnSchema.toProtobuf(proto))
            .collect(toList());
  }

  @NotNull
  public static List<ColumnSchema> getColumnSchemas() {
    ColumnSchema col1 = new ColumnSchema(
            "city",
            "varchar(4)",
            "varcher",
            0,
            "",
            "",
            "用户",
            true);

    ColumnSchema col2 = new ColumnSchema(
            "id",
            "Long",
            "Long",
            1,
            "",
            "",
            "ID",
            true);

    return Arrays.asList(col1, col2);
  }


  public static List< HddsProtos.KeyValue> getMetadataList() {
    List<HddsProtos.KeyValue> metadataList = new ArrayList<>();
    metadataList.add(HddsProtos.KeyValue.newBuilder().setKey("key1").setValue(
        "value1").build());
    metadataList.add(HddsProtos.KeyValue.newBuilder().setKey("key2").setValue(
        "value2").build());
    return metadataList;
  }


  /**
   * Add user to user table.
   * @param volumeName
   * @param ownerName
   * @param omMetadataManager
   * @throws Exception
   */
  public static void addUserToDB(String volumeName, String ownerName,
      OMMetadataManager omMetadataManager) throws Exception {

    OzoneManagerStorageProtos.PersistedUserVolumeInfo userVolumeInfo =
        omMetadataManager.getUserTable().get(
            omMetadataManager.getUserKey(ownerName));
    if (userVolumeInfo == null) {
      userVolumeInfo = OzoneManagerStorageProtos.PersistedUserVolumeInfo
          .newBuilder()
          .addVolumeNames(volumeName)
          .setObjectID(1)
          .setUpdateID(1)
          .build();
    } else {
      userVolumeInfo = userVolumeInfo.toBuilder()
          .addVolumeNames(volumeName)
          .build();
    }

    omMetadataManager.getUserTable().put(
        omMetadataManager.getUserKey(ownerName), userVolumeInfo);
  }

  /**
   * Add user to user table.
   * @param databaseName
   * @param ownerName
   * @param omMetadataManager
   * @throws Exception
   */
  public static void addDbUserToDB(String databaseName, String ownerName,
                                 OMMetadataManager omMetadataManager) throws Exception {

    OzoneManagerStorageProtos.PersistedUserDatabaseInfo userDatabaseInfo =
            omMetadataManager.getUserTableDb().get(
                    omMetadataManager.getUserKey(ownerName));
    if (userDatabaseInfo == null) {
      userDatabaseInfo = OzoneManagerStorageProtos.PersistedUserDatabaseInfo
              .newBuilder()
              .addDatabaseNames(databaseName)
              .setObjectID(1)
              .setUpdateID(1)
              .build();
    } else {
      userDatabaseInfo = userDatabaseInfo.toBuilder()
              .addDatabaseNames(databaseName)
              .build();
    }

    omMetadataManager.getUserTableDb().put(
            omMetadataManager.getUserKey(ownerName), userDatabaseInfo);
  }

  /**
   * Create OMRequest for set volume property request with owner set.
   * @param volumeName
   * @param newOwner
   * @return OMRequest
   */
  public static OMRequest createSetVolumePropertyRequest(String volumeName,
      String newOwner) {
    SetVolumePropertyRequest setVolumePropertyRequest =
        SetVolumePropertyRequest.newBuilder().setVolumeName(volumeName)
            .setOwnerName(newOwner).setModificationTime(Time.now()).build();

    return OMRequest.newBuilder().setClientId(UUID.randomUUID().toString())
        .setCmdType(OzoneManagerProtocolProtos.Type.SetVolumeProperty)
        .setSetVolumePropertyRequest(setVolumePropertyRequest).build();
  }


  /**
   * Create OMRequest for set volume property request with quota set.
   * @param volumeName
   * @param quotaInBytes
   * @param quotaInNamespace
   * @return OMRequest
   */
  public static OMRequest createSetVolumePropertyRequest(String volumeName,
      long quotaInBytes, long quotaInNamespace) {
    SetVolumePropertyRequest setVolumePropertyRequest =
        SetVolumePropertyRequest.newBuilder().setVolumeName(volumeName)
            .setQuotaInBytes(quotaInBytes)
            .setQuotaInNamespace(quotaInNamespace)
            .setModificationTime(Time.now()).build();

    return OMRequest.newBuilder().setClientId(UUID.randomUUID().toString())
        .setCmdType(OzoneManagerProtocolProtos.Type.SetVolumeProperty)
        .setSetVolumePropertyRequest(setVolumePropertyRequest).build();
  }

  /**
   * Create OMRequest for set volume property request with owner set.
   * @param databaseName
   * @param newOwner
   * @return OMRequest
   */
  public static OMRequest createSetDatabasePropertyRequest(String databaseName,
                                                         String newOwner) {
    OzoneManagerProtocolProtos.DatabaseInfo databaseInfo = OzoneManagerProtocolProtos.DatabaseInfo.newBuilder()
            .setName(databaseName)
            .setOwnerName(newOwner)
            .setAdminName(newOwner)
            .setModificationTime(Time.now())
            .build();

    SetDatabasePropertyRequest setDatabasePropertyRequest = SetDatabasePropertyRequest.newBuilder()
            .setDatabaseInfo(databaseInfo)
            .build();

    return OMRequest.newBuilder().setClientId(UUID.randomUUID().toString())
            .setCmdType(OzoneManagerProtocolProtos.Type.SetDatabaseProperty)
            .setSetDatabasePropertyRequest(setDatabasePropertyRequest).build();
  }

  /**
   * Create OMRequest for set volume property request with quota set.
   * @param databaseName
   * @param quotaInBytes
   * @param quotaInNamespace
   * @return OMRequest
   */
  public static OMRequest createSetDatabasePropertyRequest(String databaseName,
                                                         long quotaInBytes, long quotaInNamespace) {
    OzoneManagerProtocolProtos.DatabaseInfo databaseInfo = OzoneManagerProtocolProtos.DatabaseInfo.newBuilder().setName(databaseName)
            .setQuotaInBytes(quotaInBytes)
            .setQuotaInNamespace(quotaInNamespace)
            .setModificationTime(Time.now())
            .setAdminName(databaseName)
            .setOwnerName(databaseName)
            .setName(databaseName)
            .build();

    SetDatabasePropertyRequest setDatabasePropertyRequest = SetDatabasePropertyRequest.newBuilder()
                    .setDatabaseInfo(databaseInfo)
                    .build();

    return OMRequest.newBuilder().setClientId(UUID.randomUUID().toString())
            .setCmdType(OzoneManagerProtocolProtos.Type.SetDatabaseProperty)
            .setSetDatabasePropertyRequest(setDatabasePropertyRequest).build();
  }

  public static OMRequest createVolumeAddAclRequest(String volumeName,
      OzoneAcl acl) {
    AddAclRequest.Builder addAclRequestBuilder = AddAclRequest.newBuilder();
    addAclRequestBuilder.setObj(OzoneObj.toProtobuf(new OzoneObjInfo.Builder()
        .setVolumeName(volumeName)
        .setResType(ResourceType.VOLUME)
        .setStoreType(StoreType.OZONE)
        .build()));
    if (acl != null) {
      addAclRequestBuilder.setAcl(OzoneAcl.toProtobuf(acl));
    }
    return OMRequest.newBuilder().setClientId(UUID.randomUUID().toString())
        .setCmdType(OzoneManagerProtocolProtos.Type.AddAcl)
        .setAddAclRequest(addAclRequestBuilder.build()).build();
  }

  public static OMRequest createVolumeRemoveAclRequest(String volumeName,
      OzoneAcl acl) {
    RemoveAclRequest.Builder removeAclRequestBuilder =
        RemoveAclRequest.newBuilder();
    removeAclRequestBuilder.setObj(OzoneObj.toProtobuf(
        new OzoneObjInfo.Builder()
            .setVolumeName(volumeName)
            .setResType(ResourceType.VOLUME)
            .setStoreType(StoreType.OZONE)
            .build()));
    if (acl != null) {
      removeAclRequestBuilder.setAcl(OzoneAcl.toProtobuf(acl));
    }
    return OMRequest.newBuilder().setClientId(UUID.randomUUID().toString())
        .setCmdType(OzoneManagerProtocolProtos.Type.RemoveAcl)
        .setRemoveAclRequest(removeAclRequestBuilder.build()).build();
  }

  public static OMRequest createVolumeSetAclRequest(String volumeName,
      List<OzoneAcl> acls) {
    SetAclRequest.Builder setAclRequestBuilder = SetAclRequest.newBuilder();
    setAclRequestBuilder.setObj(OzoneObj.toProtobuf(new OzoneObjInfo.Builder()
        .setVolumeName(volumeName)
        .setResType(ResourceType.VOLUME)
        .setStoreType(StoreType.OZONE)
        .build()));
    if (acls != null) {
      acls.forEach(
          acl -> setAclRequestBuilder.addAcl(OzoneAcl.toProtobuf(acl)));
    }

    return OMRequest.newBuilder().setClientId(UUID.randomUUID().toString())
        .setCmdType(OzoneManagerProtocolProtos.Type.SetAcl)
        .setSetAclRequest(setAclRequestBuilder.build()).build();
  }

  // Create OMRequest for testing adding acl of bucket.
  public static OMRequest createBucketAddAclRequest(String volumeName,
      String bucketName, OzoneAcl acl) {
    AddAclRequest.Builder addAclRequestBuilder = AddAclRequest.newBuilder();
    addAclRequestBuilder.setObj(OzoneObj.toProtobuf(new OzoneObjInfo.Builder()
        .setVolumeName(volumeName).setBucketName(bucketName)
        .setResType(ResourceType.BUCKET)
        .setStoreType(StoreType.OZONE)
        .build()));

    if (acl != null) {
      addAclRequestBuilder.setAcl(OzoneAcl.toProtobuf(acl));
    }

    return OMRequest.newBuilder().setClientId(UUID.randomUUID().toString())
        .setCmdType(OzoneManagerProtocolProtos.Type.AddAcl)
        .setAddAclRequest(addAclRequestBuilder.build()).build();
  }

  // Create OMRequest for testing removing acl of bucket.
  public static OMRequest createBucketRemoveAclRequest(String volumeName,
      String bucketName, OzoneAcl acl) {
    RemoveAclRequest.Builder removeAclRequestBuilder =
        RemoveAclRequest.newBuilder();
    removeAclRequestBuilder.setObj(OzoneObj.toProtobuf(
        new OzoneObjInfo.Builder()
            .setVolumeName(volumeName).setBucketName(bucketName)
            .setResType(ResourceType.BUCKET)
            .setStoreType(StoreType.OZONE)
            .build()));

    if (acl != null) {
      removeAclRequestBuilder.setAcl(OzoneAcl.toProtobuf(acl));
    }

    return OMRequest.newBuilder().setClientId(UUID.randomUUID().toString())
        .setCmdType(OzoneManagerProtocolProtos.Type.RemoveAcl)
        .setRemoveAclRequest(removeAclRequestBuilder.build()).build();
  }

  // Create OMRequest for testing setting acls of bucket.
  public static OMRequest createBucketSetAclRequest(String volumeName,
      String bucketName, List<OzoneAcl> acls) {
    SetAclRequest.Builder setAclRequestBuilder = SetAclRequest.newBuilder();
    setAclRequestBuilder.setObj(OzoneObj.toProtobuf(new OzoneObjInfo.Builder()
        .setVolumeName(volumeName).setBucketName(bucketName)
        .setResType(ResourceType.BUCKET)
        .setStoreType(StoreType.OZONE)
        .build()));

    if (acls != null) {
      acls.forEach(
          acl -> setAclRequestBuilder.addAcl(OzoneAcl.toProtobuf(acl)));
    }

    return OMRequest.newBuilder().setClientId(UUID.randomUUID().toString())
        .setCmdType(OzoneManagerProtocolProtos.Type.SetAcl)
        .setSetAclRequest(setAclRequestBuilder.build()).build();
  }

  /**
   * Deletes key from Key table and adds it to DeletedKeys table.
   * @return the deletedKey name
   */
  public static String deleteKey(String ozoneKey,
      OMMetadataManager omMetadataManager, long trxnLogIndex)
      throws IOException {
    // Retrieve the keyInfo
    OmKeyInfo omKeyInfo = omMetadataManager.getKeyTable().get(ozoneKey);

    // Delete key from KeyTable and put in DeletedKeyTable
    omMetadataManager.getKeyTable().delete(ozoneKey);

    RepeatedOmKeyInfo repeatedOmKeyInfo =
        omMetadataManager.getDeletedTable().get(ozoneKey);

    repeatedOmKeyInfo = OmUtils.prepareKeyForDelete(omKeyInfo,
        repeatedOmKeyInfo, trxnLogIndex, true);

    omMetadataManager.getDeletedTable().put(ozoneKey, repeatedOmKeyInfo);

    return ozoneKey;
  }

  /**
   * Deletes tablet from Tablet table and adds it to DeletedTablets table.
   * @return the deletedTablet name
   */
  public static String deleteTablet(String ozoneTablet,
                                 OMMetadataManager omMetadataManager, long trxnLogIndex)
          throws IOException {
    // Retrieve the tabletInfo
    OmTabletInfo omTabletInfo = omMetadataManager.getTabletTable().get(ozoneTablet);

    // Delete tablet from tabletTable and put in DeletedTabletTable
    omMetadataManager.getTabletTable().delete(ozoneTablet);

    RepeatedOmTabletInfo repeatedOmTabletInfo =
            omMetadataManager.getDeletedTablet().get(ozoneTablet);

    repeatedOmTabletInfo = OmUtils.prepareTabletForDelete(omTabletInfo,
            repeatedOmTabletInfo, trxnLogIndex, true);

    omMetadataManager.getDeletedTablet().put(ozoneTablet, repeatedOmTabletInfo);

    return ozoneTablet;
  }

  /**
   * Create OMRequest which encapsulates InitiateMultipartUpload request.
   * @param volumeName
   * @param bucketName
   * @param keyName
   */
  public static OMRequest createInitiateMPURequest(String volumeName,
      String bucketName, String keyName) {
    MultipartInfoInitiateRequest
        multipartInfoInitiateRequest =
        MultipartInfoInitiateRequest.newBuilder().setKeyArgs(
            KeyArgs.newBuilder().setVolumeName(volumeName).setKeyName(keyName)
                .setBucketName(bucketName)).build();

    return OMRequest.newBuilder().setClientId(UUID.randomUUID().toString())
        .setCmdType(OzoneManagerProtocolProtos.Type.InitiateMultiPartUpload)
        .setInitiateMultiPartUploadRequest(multipartInfoInitiateRequest)
        .build();
  }

  /**
   * Create OMRequest which encapsulates InitiateMultipartUpload request.
   * @param volumeName
   * @param bucketName
   * @param keyName
   */
  public static OMRequest createCommitPartMPURequest(String volumeName,
      String bucketName, String keyName, long clientID, long size,
      String multipartUploadID, int partNumber) {

    // Just set dummy size.
    KeyArgs.Builder keyArgs =
        KeyArgs.newBuilder().setVolumeName(volumeName).setKeyName(keyName)
            .setBucketName(bucketName)
            .setDataSize(size)
            .setMultipartNumber(partNumber)
            .setMultipartUploadID(multipartUploadID)
            .addAllKeyLocations(new ArrayList<>());
    // Just adding dummy list. As this is for UT only.

    MultipartCommitUploadPartRequest multipartCommitUploadPartRequest =
        MultipartCommitUploadPartRequest.newBuilder()
            .setKeyArgs(keyArgs).setClientID(clientID).build();

    return OMRequest.newBuilder().setClientId(UUID.randomUUID().toString())
        .setCmdType(OzoneManagerProtocolProtos.Type.CommitMultiPartUpload)
        .setCommitMultiPartUploadRequest(multipartCommitUploadPartRequest)
        .build();
  }

  public static OMRequest createAbortMPURequest(String volumeName,
      String bucketName, String keyName, String multipartUploadID) {
    KeyArgs.Builder keyArgs =
        KeyArgs.newBuilder().setVolumeName(volumeName)
            .setKeyName(keyName)
            .setBucketName(bucketName)
            .setMultipartUploadID(multipartUploadID);

    MultipartUploadAbortRequest multipartUploadAbortRequest =
        MultipartUploadAbortRequest.newBuilder().setKeyArgs(keyArgs).build();

    return OMRequest.newBuilder().setClientId(UUID.randomUUID().toString())
        .setCmdType(OzoneManagerProtocolProtos.Type.AbortMultiPartUpload)
        .setAbortMultiPartUploadRequest(multipartUploadAbortRequest).build();
  }

  public static OMRequest createCompleteMPURequest(String volumeName,
      String bucketName, String keyName, String multipartUploadID,
      List<OzoneManagerProtocolProtos.Part> partList) {
    KeyArgs.Builder keyArgs =
        KeyArgs.newBuilder().setVolumeName(volumeName)
            .setKeyName(keyName)
            .setBucketName(bucketName)
            .setMultipartUploadID(multipartUploadID);

    MultipartUploadCompleteRequest multipartUploadCompleteRequest =
        MultipartUploadCompleteRequest.newBuilder().setKeyArgs(keyArgs)
            .addAllPartsList(partList).build();

    return OMRequest.newBuilder().setClientId(UUID.randomUUID().toString())
        .setCmdType(OzoneManagerProtocolProtos.Type.CompleteMultiPartUpload)
        .setCompleteMultiPartUploadRequest(multipartUploadCompleteRequest)
        .build();

  }

  /**
   * Create OMRequest for create volume.
   * @param volumeName
   * @param adminName
   * @param ownerName
   * @return OMRequest
   */
  public static OMRequest createVolumeRequest(String volumeName,
      String adminName, String ownerName) {
    OzoneManagerProtocolProtos.VolumeInfo volumeInfo =
        OzoneManagerProtocolProtos.VolumeInfo.newBuilder().setVolume(volumeName)
        .setAdminName(adminName).setOwnerName(ownerName)
        .setQuotaInNamespace(OzoneConsts.QUOTA_RESET).build();
    OzoneManagerProtocolProtos.CreateVolumeRequest createVolumeRequest =
        OzoneManagerProtocolProtos.CreateVolumeRequest.newBuilder()
            .setVolumeInfo(volumeInfo).build();

    return OMRequest.newBuilder().setClientId(UUID.randomUUID().toString())
        .setCmdType(OzoneManagerProtocolProtos.Type.CreateVolume)
        .setCreateVolumeRequest(createVolumeRequest).build();
  }

  /**
   * Create OMRequest for delete bucket.
   * @param volumeName
   * @param bucketName
   */
  public static OMRequest createDeleteBucketRequest(String volumeName,
      String bucketName) {
    return OMRequest.newBuilder().setDeleteBucketRequest(
        OzoneManagerProtocolProtos.DeleteBucketRequest.newBuilder()
            .setBucketName(bucketName).setVolumeName(volumeName))
        .setCmdType(OzoneManagerProtocolProtos.Type.DeleteBucket)
        .setClientId(UUID.randomUUID().toString()).build();
  }

  /**
   * Add the Key information to OzoneManager DB and cache.
   * @param omMetadataManager
   * @param omKeyInfo
   * @throws IOException
   */
  public static void addKeyToOM(final OMMetadataManager omMetadataManager,
                                final OmKeyInfo omKeyInfo) throws IOException {
    final String dbKey = omMetadataManager.getOzoneKey(
        omKeyInfo.getVolumeName(), omKeyInfo.getBucketName(),
        omKeyInfo.getKeyName());
    omMetadataManager.getKeyTable().put(dbKey, omKeyInfo);
    omMetadataManager.getKeyTable().addCacheEntry(
        new CacheKey<>(dbKey),
        new CacheValue<>(Optional.of(omKeyInfo), 1L));
  }

  /**
   * Add the Bucket information to OzoneManager DB and cache.
   * @param omMetadataManager
   * @param omBucketInfo
   * @throws IOException
   */
  public static void addBucketToOM(OMMetadataManager omMetadataManager,
      OmBucketInfo omBucketInfo) throws IOException {
    String dbBucketKey =
        omMetadataManager.getBucketKey(omBucketInfo.getVolumeName(),
            omBucketInfo.getBucketName());
    omMetadataManager.getBucketTable().put(dbBucketKey, omBucketInfo);
    omMetadataManager.getBucketTable().addCacheEntry(
        new CacheKey<>(dbBucketKey),
        new CacheValue<>(Optional.of(omBucketInfo), 1L));
  }

  /**
   * Add the table information to OzoneManager DB and cache.
   * @param omMetadataManager
   * @param omTableInfo
   * @throws IOException
   */
  public static void addMetaTableToOM(OMMetadataManager omMetadataManager,
                                   OmTableInfo omTableInfo) throws IOException {
    String dbTableKey =
            omMetadataManager.getMetaTableKey(omTableInfo.getDatabaseName(),
                    omTableInfo.getTableName());
    omMetadataManager.getMetaTable().put(dbTableKey, omTableInfo);
    omMetadataManager.getMetaTable().addCacheEntry(
            new CacheKey<>(dbTableKey),
            new CacheValue<>(Optional.of(omTableInfo), 1L));
  }

  /**
   * Add the Volume information to OzoneManager DB and Cache.
   * @param omMetadataManager
   * @param omVolumeArgs
   * @throws IOException
   */
  public static void addVolumeToOM(OMMetadataManager omMetadataManager,
      OmVolumeArgs omVolumeArgs) throws IOException {
    String dbVolumeKey =
        omMetadataManager.getVolumeKey(omVolumeArgs.getVolume());
    omMetadataManager.getVolumeTable().put(dbVolumeKey, omVolumeArgs);
    omMetadataManager.getVolumeTable().addCacheEntry(
        new CacheKey<>(dbVolumeKey),
        new CacheValue<>(Optional.of(omVolumeArgs), 1L));
  }

  /**
   * Add the Database information to OzoneManager DB and Cache.
   * @param omMetadataManager
   * @param omDatabaseArgs
   * @throws IOException
   */
  public static void addDatabaseToOM(OMMetadataManager omMetadataManager,
                                   OmDatabaseArgs omDatabaseArgs) throws IOException {
    String dbDatabaseKey =
            omMetadataManager.getDatabaseKey(omDatabaseArgs.getName());
    omMetadataManager.getDatabaseTable().put(dbDatabaseKey, omDatabaseArgs);
    omMetadataManager.getDatabaseTable().addCacheEntry(
            new CacheKey<>(dbDatabaseKey),
            new CacheValue<>(Optional.of(omDatabaseArgs), 1L));
  }

  public static void addDatabaseToDB(String databaseName,
                                     OMMetadataManager omMetadataManager) throws Exception {
    addDatabaseToDB(databaseName, UUID.randomUUID().toString(), omMetadataManager);
  }

  /**
   * Add databsae creation entry to OM DB.
   * @param databaseName
   * @param ownerName
   * @param omMetadataManager
   * @throws Exception
   */
  public static void addDatabaseToDB(String databaseName, String ownerName,
                                   OMMetadataManager omMetadataManager) throws Exception {
    OmDatabaseArgs omDatabaseArgs =
            OmDatabaseArgs.newBuilder().setCreationTime(Time.now())
                    .setName(databaseName).setAdminName(ownerName)
                    .setOwnerName(ownerName).setQuotaInBytes(Long.MAX_VALUE)
                    .setQuotaInNamespace(10000L).build();
    omMetadataManager.getDatabaseTable().put(
            omMetadataManager.getVolumeKey(databaseName), omDatabaseArgs);

    // Add to cache.
    omMetadataManager.getDatabaseTable().addCacheEntry(
            new CacheKey<>(omMetadataManager.getVolumeKey(databaseName)),
            new CacheValue<>(Optional.of(omDatabaseArgs), 1L));
  }

  /**
   * Add database creation entry to OM DB.
   * @param databaseName
   * @param omMetadataManager
   * @param quotaInBytes
   * @throws Exception
   */
  public static void addDatabaseToDB(String databaseName,
                                   OMMetadataManager omMetadataManager, long quotaInBytes) throws Exception {
    OmDatabaseArgs omDatabaseArgs =
            OmDatabaseArgs.newBuilder().setCreationTime(Time.now())
                    .setName(databaseName).setAdminName(databaseName)
                    .setOwnerName(databaseName).setQuotaInBytes(quotaInBytes)
                    .setQuotaInNamespace(10000L).build();
    omMetadataManager.getDatabaseTable().put(
            omMetadataManager.getDatabaseKey(databaseName), omDatabaseArgs);

    // Add to cache.
    omMetadataManager.getDatabaseTable().addCacheEntry(
            new CacheKey<>(omMetadataManager.getDatabaseKey(databaseName)),
            new CacheValue<>(Optional.of(omDatabaseArgs), 1L));
  }
}
