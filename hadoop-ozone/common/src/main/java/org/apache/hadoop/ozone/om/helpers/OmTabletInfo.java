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
package org.apache.hadoop.ozone.om.helpers;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TabletInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TabletLocationList;
import org.apache.hadoop.ozone.protocolPB.OMPBHelper;
import org.apache.hadoop.util.Time;
import sun.tools.jconsole.Tab;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Args for tablet block. The block instance for the key requested in putKey.
 * This is returned from OM to client, and client use class to talk to
 * datanode. Also, this is the metadata written to om.db on server side.
 */
public final class OmTabletInfo extends WithObjectID {
  private final String databaseName;
  private final String tableName;
  private final String partitionName;
  // name of tablet client specified
  private String tabletName;
  private long dataSize;
  private List<OmTabletLocationInfoGroup> tabletLocationVersions;
  private final long creationTime;
  private long modificationTime;
  private HddsProtos.ReplicationType type;
  private HddsProtos.ReplicationFactor factor;

  @SuppressWarnings("parameternumber")
  OmTabletInfo(String databaseName, String tableName, String partitionName,
               String tabletName, List<OmTabletLocationInfoGroup> versions,
               long dataSize, long creationTime, long modificationTime,
               HddsProtos.ReplicationType type,
               HddsProtos.ReplicationFactor factor,
               Map<String, String> metadata,
               long objectID, long updateID) {
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.partitionName = partitionName;
    this.tabletName = tabletName;
    this.dataSize = dataSize;
    // it is important that the versions are ordered from old to new.
    // Do this sanity check when versions got loaded on creating OmTabletInfo.
    // TODO : this is not necessary, here only because versioning is still a
    // work in-progress, remove this following check when versioning is
    // complete and prove correctly functioning
    long currentVersion = -1;
    for (OmTabletLocationInfoGroup version : versions) {
      Preconditions.checkArgument(
            currentVersion + 1 == version.getVersion());
      currentVersion = version.getVersion();
    }
    this.tabletLocationVersions = versions;
    this.creationTime = creationTime;
    this.modificationTime = modificationTime;
    this.factor = factor;
    this.type = type;
    this.metadata = metadata;
    this.objectID = objectID;
    this.updateID = updateID;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public String getTableName() {
    return tableName;
  }

  public String getPartitionName() {
    return partitionName;
  }

  public HddsProtos.ReplicationType getType() {
    return type;
  }

  public HddsProtos.ReplicationFactor getFactor() {
    return factor;
  }

  public String getTabletName() {
    return tabletName;
  }

  public void setTabletName(String tabletName) {
    this.tabletName = tabletName;
  }

  public long getDataSize() {
    return dataSize;
  }

  public void setDataSize(long size) {
    this.dataSize = size;
  }

  public synchronized OmTabletLocationInfoGroup getLatestVersionLocations() {
    return tabletLocationVersions.size() == 0? null :
        tabletLocationVersions.get(tabletLocationVersions.size() - 1);
  }

  public List<OmTabletLocationInfoGroup> getTabletLocationVersions() {
    return tabletLocationVersions;
  }

  public void updateModifcationTime() {
    this.modificationTime = Time.monotonicNow();
  }

  /**
   * updates the length of the each block in the list given.
   * This will be called when the key is being committed to OzoneManager.
   *
   * @param locationInfoList list of locationInfo
   */
  public void updateLocationInfoList(List<OmTabletLocationInfo> locationInfoList,
      boolean isMpu) {
    long latestVersion = getLatestVersionLocations().getVersion();
    OmTabletLocationInfoGroup tabletLocationInfoGroup = getLatestVersionLocations();

    // Updates the latest locationList in the latest version only with
    // given locationInfoList here.
    // TODO : The original allocated list and the updated list here may vary
    // as the containers on the Datanode on which the blocks were pre allocated
    // might get closed. The diff of blocks between these two lists here
    // need to be garbage collected in case the ozone client dies.
    tabletLocationInfoGroup.removeBlocks(latestVersion);
    // set each of the locationInfo object to the latest version
    locationInfoList.forEach(omTabletLocationInfo -> omTabletLocationInfo
        .setCreateVersion(latestVersion));
    tabletLocationInfoGroup.addAll(latestVersion, locationInfoList);
  }



  /**
   * Append a set of blocks to the latest version. Note that these blocks are
   * part of the latest version, not a new version.
   *
   * @param newLocationList the list of new blocks to be added.
   * @param updateTime if true, will update modification time.
   * @throws IOException
   */
  public synchronized void appendNewBlocks(
      List<OmTabletLocationInfo> newLocationList, boolean updateTime)
      throws IOException {
    if (tabletLocationVersions.size() == 0) {
      throw new IOException("Appending new block, but no version exist");
    }
    OmTabletLocationInfoGroup currentLatestVersion =
            tabletLocationVersions.get(tabletLocationVersions.size() - 1);
    currentLatestVersion.appendNewBlocks(newLocationList);
    if (updateTime) {
      setModificationTime(Time.now());
    }
  }

  /**
   * Add a new set of blocks. The new blocks will be added as appending a new
   * version to the all version list.
   *
   * @param newLocationList the list of new blocks to be added.
   * @param updateTime - if true, updates modification time.
   * @throws IOException
   */
  public synchronized long addNewVersion(
      List<OmTabletLocationInfo> newLocationList, boolean updateTime)
      throws IOException {
    long latestVersionNum;
    if (tabletLocationVersions.size() == 0) {
      // no version exist, these blocks are the very first version.
      tabletLocationVersions.add(new OmTabletLocationInfoGroup(0, newLocationList));
      latestVersionNum = 0;
    } else {
      // it is important that the new version are always at the tail of the list
      OmTabletLocationInfoGroup currentLatestVersion =
          tabletLocationVersions.get(tabletLocationVersions.size() - 1);
      // the new version is created based on the current latest version
      OmTabletLocationInfoGroup newVersion =
          currentLatestVersion.generateNextVersion(newLocationList);
      tabletLocationVersions.add(newVersion);
      latestVersionNum = newVersion.getVersion();
    }

    if (updateTime) {
      setModificationTime(Time.now());
    }
    return latestVersionNum;
  }

  public long getCreationTime() {
    return creationTime;
  }

  public long getModificationTime() {
    return modificationTime;
  }

  public void setModificationTime(long modificationTime) {
    this.modificationTime = modificationTime;
  }

  /**
   * Builder of OmTabletInfo.
   */
  public static class Builder {
    private String databaseName;
    private String tableName;
    private String partitionName;
    private String tabletName;
    private long dataSize;
    private List<OmTabletLocationInfoGroup> omTabletLocationInfoGroups =
        new ArrayList<>();
    private long creationTime;
    private long modificationTime;
    private HddsProtos.ReplicationType type;
    private HddsProtos.ReplicationFactor factor;
    private Map<String, String> metadata;
    private FileEncryptionInfo encInfo;
    private long objectID;
    private long updateID;

    public Builder() {
      this.metadata = new HashMap<>();
      omTabletLocationInfoGroups = new ArrayList<>();
    }

    public Builder setDatabaseName(String databaseName) {
      this.databaseName = databaseName;
      return this;
    }

    public Builder setTableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    public Builder setPartitionName(String partitionName) {
      this.partitionName = partitionName;
      return this;
    }

    public Builder setTabletName(String tabletName) {
      this.tabletName = tabletName;
      return this;
    }

    public Builder setOmTabletLocationInfos(
        List<OmTabletLocationInfoGroup> omTabletLocationInfoList) {
      if (omTabletLocationInfoList != null) {
        this.omTabletLocationInfoGroups.addAll(omTabletLocationInfoList);
      }
      return this;
    }

    public Builder addOmTabletLocationInfoGroup(OmTabletLocationInfoGroup
        omTabletLocationInfoGroup) {
      if (omTabletLocationInfoGroup != null) {
        this.omTabletLocationInfoGroups.add(omTabletLocationInfoGroup);
      }
      return this;
    }

    public Builder setDataSize(long size) {
      this.dataSize = size;
      return this;
    }

    public Builder setCreationTime(long crTime) {
      this.creationTime = crTime;
      return this;
    }

    public Builder setModificationTime(long mTime) {
      this.modificationTime = mTime;
      return this;
    }

    public Builder setReplicationFactor(HddsProtos.ReplicationFactor replFact) {
      this.factor = replFact;
      return this;
    }

    public Builder setReplicationType(HddsProtos.ReplicationType replType) {
      this.type = replType;
      return this;
    }

    public Builder addMetadata(String key, String value) {
      metadata.put(key, value);
      return this;
    }

    public Builder addAllMetadata(Map<String, String> newMetadata) {
      metadata.putAll(newMetadata);
      return this;
    }

    public Builder setObjectID(long obId) {
      this.objectID = obId;
      return this;
    }

    public Builder setUpdateID(long id) {
      this.updateID = id;
      return this;
    }

    public OmTabletInfo build() {
      return new OmTabletInfo(
          databaseName, tableName, partitionName, tabletName, omTabletLocationInfoGroups,
          dataSize, creationTime, modificationTime, type, factor, metadata
          ,objectID, updateID);
    }
  }

  /**
   * For network transmit.
   * @return
   */
  public TabletInfo getProtobuf(int clientVersion) {
    return getProtobuf(false, clientVersion);
  }

  /**
   *
   * @param ignorePipeline true for persist to DB, false for network transmit.
   * @return
   */
  public TabletInfo getProtobuf(boolean ignorePipeline, int clientVersion) {
    long latestVersion = tabletLocationVersions.size() == 0 ? -1 :
        tabletLocationVersions.get(tabletLocationVersions.size() - 1).getVersion();

    List<TabletLocationList> tabletLocations = new ArrayList<>();
    for (OmTabletLocationInfoGroup locationInfoGroup : tabletLocationVersions) {
      tabletLocations.add(locationInfoGroup.getProtobuf(
          ignorePipeline, clientVersion));
    }

    TabletInfo.Builder kb = TabletInfo.newBuilder()
        .setDatabaseName(databaseName)
        .setTableName(tableName)
        .setPartitionName(partitionName)
        .setTabletName(tabletName)
        .setDataSize(dataSize)
        .setFactor(factor)
        .setType(type)
        .setLatestVersion(latestVersion)
        .addAllTabletLocationList(tabletLocations)
        .setCreationTime(creationTime)
        .setModificationTime(modificationTime)
        .addAllMetadata(KeyValueUtil.toProtobuf(metadata))
        .setObjectID(objectID)
        .setUpdateID(updateID);
    return kb.build();
  }

  public static OmTabletInfo getFromProtobuf(TabletInfo TabletInfo) {
    if (TabletInfo == null) {
      return null;
    }

    List<OmTabletLocationInfoGroup> omTabletLocationInfos = new ArrayList<>();
    for (TabletLocationList tabletLocationList : TabletInfo.getTabletLocationListList()) {
      omTabletLocationInfos.add(
          OmTabletLocationInfoGroup.getFromProtobuf(tabletLocationList));
    }

    Builder builder = new Builder()
        .setDatabaseName(TabletInfo.getDatabaseName())
        .setTableName(TabletInfo.getTableName())
        .setPartitionName(TabletInfo.getPartitionName())
        .setTabletName(TabletInfo.getTabletName())
        .setOmTabletLocationInfos(omTabletLocationInfos)
        .setDataSize(TabletInfo.getDataSize())
        .setCreationTime(TabletInfo.getCreationTime())
        .setModificationTime(TabletInfo.getModificationTime())
        .setReplicationType(TabletInfo.getType())
        .setReplicationFactor(TabletInfo.getFactor())
        .addAllMetadata(KeyValueUtil.getFromProtobuf(TabletInfo.getMetadataList()));
    if (TabletInfo.hasObjectID()) {
      builder.setObjectID(TabletInfo.getObjectID());
    }
    if (TabletInfo.hasUpdateID()) {
      builder.setUpdateID(TabletInfo.getUpdateID());
    }
    return builder.build();
  }

  @Override
  public String getObjectInfo() {
    return "OMTabletInfo{" +
        "database='" + databaseName + '\'' +
        ", table='" + tableName + '\'' +
        ", partition='" + partitionName + '\'' +
        ", tablet='" + tabletName + '\'' +
        ", dataSize='" + dataSize + '\'' +
        ", creationTime='" + creationTime + '\'' +
        ", type='" + type + '\'' +
        ", factor='" + factor + '\'' +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    OmTabletInfo omTabletInfo = (OmTabletInfo) o;
    return dataSize == omTabletInfo.dataSize &&
        creationTime == omTabletInfo.creationTime &&
        modificationTime == omTabletInfo.modificationTime &&
        databaseName.equals(omTabletInfo.databaseName) &&
        tableName.equals(omTabletInfo.tableName) &&
        partitionName.equals(omTabletInfo.partitionName) &&
        tabletName.equals(omTabletInfo.tabletName) &&
        Objects
            .equals(tabletLocationVersions, omTabletInfo.tabletLocationVersions) &&
        type == omTabletInfo.type &&
        factor == omTabletInfo.factor &&
        Objects.equals(metadata, omTabletInfo.metadata) &&
        objectID == omTabletInfo.objectID &&
        updateID == omTabletInfo.updateID;
  }

  @Override
  public int hashCode() {
    return Objects.hash(databaseName, tableName, tabletName);
  }

  /**
   * Return a new copy of the object.
   */
  public OmTabletInfo copyObject() {
    OmTabletInfo.Builder builder = new OmTabletInfo.Builder()
        .setDatabaseName(databaseName)
        .setTableName(tableName)
        .setPartitionName(partitionName)
        .setTabletName(tabletName)
        .setCreationTime(creationTime)
        .setModificationTime(modificationTime)
        .setDataSize(dataSize)
        .setReplicationType(type)
        .setReplicationFactor(factor)
        .setObjectID(objectID).setUpdateID(updateID);


    tabletLocationVersions.forEach(tabletLocationVersion ->
        builder.addOmTabletLocationInfoGroup(
            new OmTabletLocationInfoGroup(tabletLocationVersion.getVersion(),
                tabletLocationVersion.getLocationList())));

    if (metadata != null) {
      metadata.forEach((k, v) -> builder.addMetadata(k, v));
    }

    return builder.build();
  }
}
