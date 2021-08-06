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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.Auditable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Args for tablet. Client use this to specify tablet's attributes on  tablet creation
 * (putTablet()).
 */
public final class OmTabletArgs implements Auditable {
  private final String databaseName;
  private final String tableName;
  private final String partitionName;
  private final String tabletName;
  private long dataSize;
  private final ReplicationType type;
  private final ReplicationFactor factor;
  private List<OmTabletLocationInfo> locationInfoList;
  private Map<String, String> metadata;
  private boolean refreshPipeline;
  private boolean sortDatanodesInPipeline;
  private List<OzoneAcl> acls;

  @SuppressWarnings("parameternumber")
  private OmTabletArgs(String databaseName, String tableName, String partitionName, String tabletName,
                       long dataSize, ReplicationType type, ReplicationFactor factor,
                       List<OmTabletLocationInfo> locationInfoList,
                       Map<String, String> metadataMap, boolean refreshPipeline,
                       List<OzoneAcl> acls, boolean sortDatanode) {
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.partitionName = partitionName;
    this.tabletName = tabletName;
    this.dataSize = dataSize;
    this.type = type;
    this.factor = factor;
    this.locationInfoList = locationInfoList;
    this.metadata = metadataMap;
    this.refreshPipeline = refreshPipeline;
    this.acls = acls;
    this.sortDatanodesInPipeline = sortDatanode;
  }

  public ReplicationType getType() {
    return type;
  }

  public ReplicationFactor getFactor() {
    return factor;
  }

  public List<OzoneAcl> getAcls() {
    return acls;
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

  public String getTabletName() {
    return tabletName;
  }

  public long getDataSize() {
    return dataSize;
  }

  public void setDataSize(long size) {
    dataSize = size;
  }

  public Map<String, String> getMetadata() {
    return metadata;
  }

  public void setMetadata(Map<String, String> metadata) {
    this.metadata = metadata;
  }

  public void setLocationInfoList(List<OmTabletLocationInfo> locationInfoList) {
    this.locationInfoList = locationInfoList;
  }

  public List<OmTabletLocationInfo> getLocationInfoList() {
    return locationInfoList;
  }

  public boolean getRefreshPipeline() {
    return refreshPipeline;
  }

  public boolean getSortDatanodes() {
    return sortDatanodesInPipeline;
  }

  @Override
  public Map<String, String> toAuditMap() {
    Map<String, String> auditMap = new LinkedHashMap<>();
    auditMap.put(OzoneConsts.DATABASE, this.databaseName);
    auditMap.put(OzoneConsts.TABLE, this.tableName);
    auditMap.put(OzoneConsts.PARTITION, this.partitionName);
    auditMap.put(OzoneConsts.TABLET, this.tabletName);
    auditMap.put(OzoneConsts.DATA_SIZE, String.valueOf(this.dataSize));
    auditMap.put(OzoneConsts.REPLICATION_TYPE,
        (this.type != null) ? this.type.name() : null);
    auditMap.put(OzoneConsts.REPLICATION_FACTOR,
        (this.factor != null) ? this.factor.name() : null);
    return auditMap;
  }

  @VisibleForTesting
  public void addLocationInfo(OmTabletLocationInfo locationInfo) {
    if (this.locationInfoList == null) {
      locationInfoList = new ArrayList<>();
    }
    locationInfoList.add(locationInfo);
  }

  public OmTabletArgs.Builder toBuilder() {
    return new OmTabletArgs.Builder()
        .setDatabaseName(databaseName)
        .setTableName(tableName)
        .setPartitionName(partitionName)
        .setTabletName(tabletName)
        .setDataSize(dataSize)
        .setType(type)
        .setFactor(factor)
        .setLocationInfoList(locationInfoList)
        .addAllMetadata(metadata)
        .setRefreshPipeline(refreshPipeline)
        .setSortDatanodesInPipeline(sortDatanodesInPipeline)
        .setAcls(acls);
  }

  /**
   * Builder class of OmKeyArgs.
   */
  public static class Builder {
    private String databaseName;
    private String tableName;
    private String partitionName;
    private String tabletName;
    private long dataSize;
    private ReplicationType type;
    private ReplicationFactor factor;
    private List<OmTabletLocationInfo> locationInfoList;
    private Map<String, String> metadata = new HashMap<>();
    private boolean refreshPipeline;
    private boolean sortDatanodesInPipeline;
    private List<OzoneAcl> acls;

    public Builder setDatabaseName(String database) {
      this.databaseName = database;
      return this;
    }

    public Builder setTableName(String table) {
      this.tableName = table;
      return this;
    }

    public Builder setPartitionName(String partition) {
      this.partitionName = partition;
      return this;
    }

    public Builder setTabletName(String tablet) {
      this.tableName = tablet;
      return this;
    }

    public Builder setDataSize(long size) {
      this.dataSize = size;
      return this;
    }

    public Builder setType(ReplicationType replicationType) {
      this.type = replicationType;
      return this;
    }

    public Builder setFactor(ReplicationFactor replicationFactor) {
      this.factor = replicationFactor;
      return this;
    }

    public Builder setLocationInfoList(List<OmTabletLocationInfo> locationInfos) {
      this.locationInfoList = locationInfos;
      return this;
    }

    public Builder setAcls(List<OzoneAcl> listOfAcls) {
      this.acls = listOfAcls;
      return this;
    }

    public Builder addMetadata(String key, String value) {
      this.metadata.put(key, value);
      return this;
    }

    public Builder addAllMetadata(Map<String, String> metadatamap) {
      this.metadata.putAll(metadatamap);
      return this;
    }

    public Builder setRefreshPipeline(boolean refresh) {
      this.refreshPipeline = refresh;
      return this;
    }

    public Builder setSortDatanodesInPipeline(boolean sort) {
      this.sortDatanodesInPipeline = sort;
      return this;
    }

    public OmTabletArgs build() {
      return new OmTabletArgs(databaseName, tableName, partitionName, tabletName,
          dataSize, type, factor, locationInfoList, metadata, refreshPipeline, acls,
          sortDatanodesInPipeline);
    }

  }
}
