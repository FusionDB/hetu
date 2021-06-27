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
import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TableInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PartitionInfo;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.stream.Collectors.toList;

/**
 * A class that encapsulates Partition Info.
 */
public final class OmPartitionInfo extends WithObjectID implements Auditable {
  /**
   * Name of the database in which the partition belongs to.
   */
  private final String databaseName;
  /**
   * Name of the partition.
   */
  private final String tableName;
  /**
   * Name of the partition.
   */
  private final String partitionName;
  /**
   * Name of the partition.
   */
  private final String partitionValue;
  /**
   * Rows of the partition.
   */
  private final long rows;
  /**
   * Buckets of the partition.
   */
  private final int buckets;
  /**
   * Partition Version flag.
   */
  private Boolean isVersionEnabled;
  /**
   * Creation time of partition.
   */
  private final long creationTime;
  /**
   * modification time of table.
   */
  private long modificationTime;
  /**
   * Table of sizeInBytes
   */
  private long sizeInBytes;
  /**
   * Partition of storageType
   */
  private StorageType storageType;

  /**
   * Private constructor, constructed via builder.
   * @param databaseName - Database name.
   * @param tableName - Table name.
   * @param partitionName - Partition name.
   * @param isVersionEnabled - Bucket version flag.
   * @param creationTime - Bucket creation time.
   * @param modificationTime - Bucket modification time.
   * @param metadata - metadata.
   */
  @SuppressWarnings("checkstyle:ParameterNumber")
  private OmPartitionInfo(String databaseName,
                          String tableName,
                          String partitionName,
                          String partitionValue,
                          long rows,
                          int buckets,
                          boolean isVersionEnabled,
                          long creationTime,
                          long modificationTime,
                          long objectID,
                          long updateID,
                          Map<String, String> metadata,
                          long sizeInBytes,
                          StorageType storageType) {
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.partitionName = partitionName;
    this.partitionValue = partitionValue;
    this.rows = rows;
    this.buckets = buckets;
    this.isVersionEnabled = isVersionEnabled;
    this.creationTime = creationTime;
    this.modificationTime = modificationTime;
    this.objectID = objectID;
    this.updateID = updateID;
    this.metadata = metadata;
    this.sizeInBytes = sizeInBytes;
    this.storageType = storageType;
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
   * Returns true if partition version is enabled, else false.
   * @return isVersionEnabled
   */
  public boolean getIsVersionEnabled() {
    return isVersionEnabled;
  }

  /**
   * Returns creation time.
   *
   * @return long
   */
  public long getCreationTime() {
    return creationTime;
  }

  /**
   * Returns modification time.
   * @return long
   */
  public long getModificationTime() {
    return modificationTime;
  }

  /**
   * Returns partition use capacity in bytes
   * @return
   */
  public long getSizeInBytes() {
    return sizeInBytes;
  }

  /**
   * Returns the partition name.
   * @return String
   */
  public String getPartitionName() {
    return partitionName;
  }

  /**
   * Returns the partition value.
   * @return
   */
  public String getPartitionValue() {
    return partitionValue;
  }

  /**
   * Returns the storage type.
   * @return
   */
  public StorageType getStorageType() {
    return storageType;
  }

  /**
   * Returns the partitions rows
   * @return
   */
  public long getRows() {
    return rows;
  }

  /**
   * Returns the partition buckets
   * @return
   */
  public int getBuckets() {
    return buckets;
  }

  /**
   * Returns new builder class that builds a OmPartitionInfo.
   *
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
    auditMap.put(OzoneConsts.PARTITION_VALUE, this.partitionValue);
    auditMap.put(OzoneConsts.ROWS, String.valueOf(this.rows));
    auditMap.put(OzoneConsts.BUCKETS, String.valueOf(this.buckets));
    auditMap.put(OzoneConsts.GDPR_FLAG,
        this.metadata.get(OzoneConsts.GDPR_FLAG));
    auditMap.put(OzoneConsts.IS_VERSION_ENABLED,
        String.valueOf(this.isVersionEnabled));
    auditMap.put(OzoneConsts.CREATION_TIME, String.valueOf(this.creationTime));
    auditMap.put(OzoneConsts.MODIFICATION_TIME,
        String.valueOf(this.modificationTime));
    auditMap.put(OzoneConsts.SIZE_IN_BYTES, String.valueOf(this.sizeInBytes));
    auditMap.put(OzoneConsts.STORAGE_TYPE, storageType.name());
    return auditMap;
  }

  /**
   * Return a new copy of the object.
   */
  public OmPartitionInfo copyObject() {
    Builder builder = toBuilder();
    return builder.build();
  }

  public Builder toBuilder() {
    return new Builder()
        .setDatabaseName(databaseName)
        .setTableName(tableName)
        .setPartitionName(partitionName)
        .setPartitionValue(partitionValue)
        .setRows(rows)
        .setBuckets(buckets)
        .setIsVersionEnabled(isVersionEnabled)
        .setCreationTime(creationTime)
        .setModificationTime(modificationTime)
        .setObjectID(objectID)
        .setUpdateID(updateID)
        .addAllMetadata(metadata)
        .setStorageType(storageType)
        .setSizeInBytes(sizeInBytes);
  }

  public void incrUsedBytes(long bytes) {
        this.sizeInBytes += bytes;
  }

  /**
   * Builder for OmBucketInfo.
   */
  public static class Builder {
    private String databaseName;
    private String tableName;
    private String partitionName;
    private String partitionValue;
    private long rows;
    private int buckets;
    private Boolean isVersionEnabled;
    private long creationTime;
    private long modificationTime;
    private long sizeInBytes;
    private long objectID;
    private long updateID;
    private Map<String, String> metadata;
    private StorageType storageType;

    public Builder() {
      //Default values
      this.isVersionEnabled = false;
      this.rows = 0;
      this.metadata = new HashMap<>();
      this.sizeInBytes = OzoneConsts.USED_CAPACITY_IN_BYTES_RESET;
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

    public Builder setRows(long rows) {
      this.rows = rows;
      return this;
    }

    public Builder setBuckets(int buckets) {
      this.buckets = buckets;
      return this;
    }

    public Builder setCreationTime(long createdOn) {
      this.creationTime = createdOn;
      return this;
    }

    public Builder setModificationTime(long modifiedOn) {
      this.modificationTime = modifiedOn;
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

    public Builder addMetadata(String key, String value) {
      metadata.put(key, value);
      return this;
    }

    public Builder addAllMetadata(Map<String, String> additionalMetadata) {
      if (additionalMetadata != null) {
        metadata.putAll(additionalMetadata);
      }
      return this;
    }

    public Builder setSizeInBytes(long sizeInBytes) {
      this.sizeInBytes = sizeInBytes;
      return this;
    }

    public Builder setStorageType(StorageType storageType) {
      this.storageType = storageType;
      return this;
    }

    /**
     * Constructs the OmPartitionInfo.
     * @return instance of OmPartitionInfo.
     */
    public OmPartitionInfo build() {
      Preconditions.checkNotNull(databaseName);
      Preconditions.checkNotNull(tableName);
      Preconditions.checkNotNull(partitionName);
      Preconditions.checkNotNull(partitionValue);
      Preconditions.checkNotNull(buckets);
      Preconditions.checkNotNull(rows);
      Preconditions.checkNotNull(isVersionEnabled);
      Preconditions.checkNotNull(storageType);

      return new OmPartitionInfo(databaseName, tableName, partitionName,
          partitionValue, rows, buckets, isVersionEnabled,
          creationTime, modificationTime, objectID, updateID,
          metadata, sizeInBytes, storageType);
    }
  }

  /**
   * Creates PartitionInfo protobuf from OmPartitionInfo.
   */
  public PartitionInfo getProtobuf() {
    PartitionInfo.Builder bib =  PartitionInfo.newBuilder()
        .setDatabaseName(databaseName)
        .setTableName(tableName)
        .setPartitionName(partitionName)
        .setPartitionValue(partitionValue)
        .setRows(rows)
        .setBuckets(buckets)
        .setIsVersionEnabled(isVersionEnabled)
        .setCreationTime(creationTime)
        .setModificationTime(modificationTime)
        .setObjectID(objectID)
        .setUpdateID(updateID)
        .setSizeInBytes(sizeInBytes)
        .setStorageType(storageType.toProto());
    return bib.build();
  }

  /**
   * Parses PartitionInfo protobuf and creates OmPartitionInfo.
   * @param partitionInfo
   * @return instance of OmPartitionInfo
   */
  public static OmPartitionInfo getFromProtobuf(PartitionInfo partitionInfo) {
    OmPartitionInfo.Builder obib = OmPartitionInfo.newBuilder()
        .setDatabaseName(partitionInfo.getDatabaseName())
        .setTableName(partitionInfo.getTableName())
        .setPartitionName(partitionInfo.getPartitionName())
        .setPartitionValue(partitionInfo.getPartitionValue())
        .setRows(partitionInfo.getRows())
        .setBuckets(partitionInfo.getBuckets())
        .setIsVersionEnabled(partitionInfo.getIsVersionEnabled())
        .setCreationTime(partitionInfo.getCreationTime())
        .setModificationTime(partitionInfo.getModificationTime())
        .setSizeInBytes(partitionInfo.getSizeInBytes())
        .setStorageType(StorageType.valueOf(partitionInfo.getStorageType()));

    if (partitionInfo.hasObjectID()) {
      obib.setObjectID(partitionInfo.getObjectID());
    }
    if (partitionInfo.hasUpdateID()) {
      obib.setUpdateID(partitionInfo.getUpdateID());
    }
    if (partitionInfo.getMetadataList() != null) {
      obib.addAllMetadata(KeyValueUtil
          .getFromProtobuf(partitionInfo.getMetadataList()));
    }
    return obib.build();
  }

  @Override
  public String getObjectInfo() {
    return "OmPartitionInfo{" +
            "databaseName='" + databaseName + '\'' +
            ", tableName='" + tableName + '\'' +
            ", partitionName='" + partitionName + '\'' +
            ", partitionValue='" + partitionValue + '\'' +
            ", rows=" + rows +
            ", buckets=" + buckets +
            ", isVersionEnabled=" + isVersionEnabled +
            ", creationTime=" + creationTime +
            ", modificationTime=" + modificationTime +
            ", sizeInBytes=" + sizeInBytes +
            ", storageType=" + storageType +
            '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    OmPartitionInfo that = (OmPartitionInfo) o;
    return rows == that.rows &&
            buckets == that.buckets &&
            creationTime == that.creationTime &&
            modificationTime == that.modificationTime &&
            sizeInBytes == that.sizeInBytes &&
            databaseName.equals(that.databaseName) &&
            tableName.equals(that.tableName) &&
            partitionName.equals(that.partitionName) &&
            partitionValue.equals(that.partitionValue) &&
            isVersionEnabled.equals(that.isVersionEnabled);
  }

  @Override
  public int hashCode() {
    return Objects.hash(databaseName, tableName, partitionName);
  }

  @Override
  public String toString() {
    return "OmPartitionInfo{" +
            "databaseName='" + databaseName + '\'' +
            ", tableName='" + tableName + '\'' +
            ", partitionName='" + partitionName + '\'' +
            ", partitionValue='" + partitionValue + '\'' +
            ", rows=" + rows +
            ", buckets=" + buckets +
            ", isVersionEnabled=" + isVersionEnabled +
            ", creationTime=" + creationTime +
            ", modificationTime=" + modificationTime +
            ", sizeInBytes=" + sizeInBytes +
            ", storageType=" + storageType +
            '}';
  }
}
