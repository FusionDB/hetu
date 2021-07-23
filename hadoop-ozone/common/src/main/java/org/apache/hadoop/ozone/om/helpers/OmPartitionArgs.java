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
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.Auditable;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PartitionArgs;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A class that encapsulates Table Arguments.
 */
public final class OmPartitionArgs extends WithMetadata implements Auditable {
  /**
   * Name of the database in which the table belongs to.
   */
  private final String databaseName;
  /**
   * Name of the table.
   */
  private final String tableName;
  /**
   * Name of the partition.
   */
  private final String partitionName;
  /**
   * Name of the partition value.
   */
  private final String partitionValue;
  /**
   * Partition Version flag.
   */
  private Boolean isVersionEnabled;
  /**
   * Type of storage to be used for this partition.
   * [RAM_DISK, SSD, DISK, ARCHIVE]
   */
  private StorageType storageType;

  /**
   * Partition num replicas
   */
  private int numReplicas;

  private long sizeInBytes;

  private long rows;

  /**
   * Private constructor, constructed via builder.
   * @param databaseName - Database name.
   * @param tableName - Table name.
   * @param partitionName - Partition name.
   * @param isVersionEnabled - Table version flag.
   * @param storageType - Storage type to be used.
   * @param sizeInBytes Table size in bytes.
   */
  private OmPartitionArgs(String databaseName, String tableName,
                          String partitionName, String partitionValue,
                          Boolean isVersionEnabled, StorageType storageType,
                          int numReplicas, long rows,
                          Map<String, String> metadata, long sizeInBytes) {
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.partitionName = partitionName;
    this.partitionValue = partitionValue;
    this.isVersionEnabled = isVersionEnabled;
    this.storageType = storageType;
    this.metadata = metadata;
    this.rows = rows;
    this.numReplicas = numReplicas;
    this.sizeInBytes = sizeInBytes;
  }

  /**
   * Returns the Database Name.
   * @return String.
   */
  public String getDatabaseName() {
    return databaseName;
  }

  /**
   * Returns the Table Name.
   * @return String
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * Returns the Partition Name.
   * @return String
   */
  public String getPartitionName() {
    return partitionName;
  }

  /**
   * Returns the Partition Value.
   * @return String
   */
  public String getPartitionValue() {
    return partitionValue;
  }

  /**
   * Returns true if table version is enabled, else false.
   * @return isVersionEnabled
   */
  public Boolean getIsVersionEnabled() {
    return isVersionEnabled;
  }

  /**
   * Returns the type of storage to be used.
   * @return StorageType
   */
  public StorageType getStorageType() {
    return storageType;
  }

  /**
   * Returns Partition used capacity in bytes.
   * @return sizeInBytes.
   */
  public long getSizeInBytes() {
    return sizeInBytes;
  }

  /**
   * Returns Table rows
   * @return
   */
  public Long getRows() {
    return rows;
  }

  /**
   * Returns Table Num Replicas
   * @return
   */
  public int getNumReplicas() {
    return numReplicas;
  }

  /**
   * Returns new builder class that builds a OmTableArgs.
   * @return Builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  @Override
  public Map<String, String> toAuditMap() {
    Map<String, String> auditMap = new LinkedHashMap<>();
    auditMap.put(OzoneConsts.DATABASE, this.databaseName);
    auditMap.put(OzoneConsts.TABLE, this.tableName);
    auditMap.put(OzoneConsts.PARTITION, this.partitionName);
    auditMap.put(OzoneConsts.GDPR_FLAG,
        this.metadata.get(OzoneConsts.GDPR_FLAG));
    auditMap.put(OzoneConsts.IS_VERSION_ENABLED,
                String.valueOf(this.isVersionEnabled));
    if(this.storageType != null){
      auditMap.put(OzoneConsts.STORAGE_TYPE, this.storageType.name());
    }
    return auditMap;
  }

  /**
   * Builder for OmTableArgs.
   */
  public static class Builder {
    private String databaseName;
    private String tableName;
    private String partitionName;
    private String partitionValue;
    private Boolean isVersionEnabled;
    private StorageType storageType;
    private Map<String, String> metadata;
    private int numReplicas;
    private long rows;
    private long sizeInBytes;

    /**
     * Constructs a builder.
     */
    public Builder() {
      sizeInBytes = OzoneConsts.USED_CAPACITY_IN_BYTES_RESET;
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

    public Builder setPartitionValue(String partitionValue) {
      this.partitionValue = partitionValue;
      return this;
    }

    public Builder setIsVersionEnabled(Boolean versionFlag) {
      this.isVersionEnabled = versionFlag;
      return this;
    }

    public Builder addMetadata(Map<String, String> metadataMap) {
      this.metadata = metadataMap;
      return this;
    }

    public Builder setStorageType(StorageType storage) {
      this.storageType = storage;
      return this;
    }

    public Builder setNumReplicas(int numReplicas) {
      this.numReplicas = numReplicas;
      return this;
    }

    public Builder setRows(long rows) {
      this.rows= rows;
      return this;
    }

    public Builder setSizeInBytes(long sizeInBytes) {
      this.sizeInBytes = sizeInBytes;
      return this;
    }

    /**
     * Constructs the OmTableArgs.
     * @return instance of OmTableArgs.
     */
    public OmPartitionArgs build() {
      Preconditions.checkNotNull(databaseName);
      Preconditions.checkNotNull(tableName);
      Preconditions.checkNotNull(partitionName);
      Preconditions.checkNotNull(partitionValue);
      Preconditions.checkNotNull(numReplicas);
      return new OmPartitionArgs(databaseName, tableName, partitionName,
              partitionValue, isVersionEnabled,
          storageType, numReplicas, rows, metadata, sizeInBytes);
    }
  }

  /**
   * Creates PartitionArgs protobuf from OmPartitionArgs.
   */
  public PartitionArgs getProtobuf() {
    PartitionArgs.Builder builder = PartitionArgs.newBuilder();
    builder.setDatabaseName(databaseName)
        .setTableName(tableName)
        .setPartitionName(partitionName)
        .setPartitionValue(partitionValue)
        .setNumReplicas(numReplicas);
    if(isVersionEnabled != null) {
      builder.setIsVersionEnabled(isVersionEnabled);
    }
    if(storageType != null) {
      builder.setStorageType(storageType.toProto());
    }
    if(rows >= 0L) {
      builder.setRows(rows);
    }
    if(sizeInBytes > 0 || sizeInBytes == OzoneConsts.USED_CAPACITY_IN_BYTES_RESET) {
      builder.setSizeInBytes(sizeInBytes);
    }
    return builder.build();
  }

  /**
   * Parses PartitionInfo protobuf and creates OmPartitionArgs.
   * @param partitionArgs
   * @return instance of OmPartitionArgs
   */
  public static OmPartitionArgs getFromProtobuf(PartitionArgs partitionArgs) {
    return new OmPartitionArgs(partitionArgs.getDatabaseName(),
            partitionArgs.getTableName(),
            partitionArgs.getPartitionName(),
            partitionArgs.getPartitionValue(),
            partitionArgs.hasIsVersionEnabled() ? partitionArgs.getIsVersionEnabled() : null,
            partitionArgs.hasStorageType() ? StorageType.valueOf(
            partitionArgs.getStorageType()) : null,
            partitionArgs.getNumReplicas(),
            partitionArgs.getRows(),
            KeyValueUtil.getFromProtobuf(partitionArgs.getMetadataList()),
            partitionArgs.getSizeInBytes());
  }
}
