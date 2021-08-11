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
import org.apache.hadoop.hetu.hm.meta.table.ColumnKey;
import org.apache.hadoop.hetu.hm.meta.table.ColumnSchema;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
        .TableInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
        .TableInfo.StorageEngineProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
        .TableInfo.PartitionsProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
        .TableInfo.DistributedKeyProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
        .ColumnSchemaProto;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.stream.Collectors.toList;

/**
 * A class that encapsulates Table Info.
 */
public final class OmTableInfo extends WithObjectID implements Auditable {
  /**
   * Name of the database in which the table belongs to.
   */
  private final String databaseName;
  /**
   * Name of the table.
   */
  private final String tableName;
  /**
   * Schema of the table.
   */
  private List<ColumnSchema> columns;
  /**
   * Table storage engine: lucene or parquet
   */
  private final StorageEngineProto storageEngine;

  /**
   * Table Version flag.
   */
  private Boolean isVersionEnabled;
  /**
   * Type of storage to be used for this table.
   * [RAM_DISK, SSD, DISK, ARCHIVE]
   */
  private StorageType storageType;
  /**
   * Creation time of table.
   */
  private final long creationTime;
  /**
   * modification time of table.
   */
  private long modificationTime;
  /**
   * Table num replica: 1 or 3 or 2n+1
   */
  private int numReplicas;
  /**
   * Table of partitions
   */
  private PartitionsProto partitions;
  /**
   * Table of usedBytes
   */
  private long usedInBytes;
  /**
   * Table of quotaBytes
   */
  private long quotaInBytes;
  /**
   * Table of quotaInNamespace
   */
  public long quotaInNamespace;

  /**
   * Table of column key
   */
  private ColumnKey columnKey;

  /**
   * Table of distributeKey
   */
  private DistributedKeyProto distributedKeyProto;

  /**
   * Private constructor, constructed via builder.
   * @param databaseName - Database name.
   * @param tableName - Table name.
   * @param isVersionEnabled - Bucket version flag.
   * @param storageType - Storage type to be used.
   * @param creationTime - Bucket creation time.
   * @param modificationTime - Bucket modification time.
   * @param metadata - metadata.
   */
  @SuppressWarnings("checkstyle:ParameterNumber")
  private OmTableInfo(String databaseName,
                      String tableName,
                      boolean isVersionEnabled,
                      StorageType storageType,
                      List<ColumnSchema> columns,
                      ColumnKey columnKey,
                      StorageEngineProto storageEngine,
                      int numReplicas,
                      PartitionsProto partitions,
                      DistributedKeyProto distributedKeyProto,
                      long creationTime,
                      long modificationTime,
                      long objectID,
                      long updateID,
                      Map<String, String> metadata,
                      long usedBytes,
                      long quotaInBytes,
                      long quotaInNamespace) {
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.isVersionEnabled = isVersionEnabled;
    this.storageType = storageType;
    this.creationTime = creationTime;
    this.modificationTime = modificationTime;
    this.objectID = objectID;
    this.updateID = updateID;
    this.metadata = metadata;
    this.columns = columns;
    this.columnKey = columnKey;
    this.storageEngine = storageEngine;
    this.numReplicas = numReplicas;
    this.partitions = partitions;
    this.distributedKeyProto = distributedKeyProto;
    this.usedInBytes = usedInBytes;
    this.quotaInBytes = quotaInBytes;
    this.quotaInNamespace = quotaInNamespace;
  }

  /**
   * Returns the Databse Name.
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
   * Returns true if table version is enabled, else false.
   * @return isVersionEnabled
   */
  public boolean getIsVersionEnabled() {
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
   * Returns table schema
   * @return Field
   */
  public List<ColumnSchema> getColumns() {
    return columns;
  }

  /**
   * Returns StorageEngine format
   * @return
   */
  public StorageEngineProto getStorageEngine() {
    return storageEngine;
  }

  /**
   * Returns table num replicas is 2n+1
   * @return
   */
  public int getNumReplicas() {
    return numReplicas;
  }

  /**
   * Returns table partitions info
   * @return
   */
  public PartitionsProto getPartitions() {
    return partitions;
  }

  /**
   * Returns table used capacity in bytes
   * @return
   */
  public long getUsedInBytes() {
    return usedInBytes;
  }

  public long getQuotaInNamespace() {
    return quotaInNamespace;
  }

  public long getQuotaInBytes() {
    return quotaInBytes;
  }

  /**
   * Returns table column key
   * @return
   */
  public ColumnKey getColumnKey() {
    return columnKey;
  }

  /**
   * Returns table distributed key
   * @return
   */
  public DistributedKeyProto getDistributedKeyProto() {
    return distributedKeyProto;
  }

  /**
   * Returns new builder class that builds a OmTableInfo.
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
    auditMap.put(OzoneConsts.GDPR_FLAG,
        this.metadata.get(OzoneConsts.GDPR_FLAG));
    auditMap.put(OzoneConsts.IS_VERSION_ENABLED,
        String.valueOf(this.isVersionEnabled));
    auditMap.put(OzoneConsts.STORAGE_TYPE,
        (this.storageType != null) ? this.storageType.name() : null);
    auditMap.put(OzoneConsts.CREATION_TIME, String.valueOf(this.creationTime));
    auditMap.put(OzoneConsts.MODIFICATION_TIME,
        String.valueOf(this.modificationTime));
    auditMap.put(OzoneConsts.TABLE_SCHEMA, String.valueOf(this.columns));
    auditMap.put(OzoneConsts.COLUMN_KEY, columnKey.toString());
    auditMap.put(OzoneConsts.DISTRIBUTED_KEY, this.distributedKeyProto.toString());
    auditMap.put(OzoneConsts.STORAGE_ENGINE, String.valueOf(this.storageEngine));
    auditMap.put(OzoneConsts.NUM_REPLICAS, String.valueOf(this.numReplicas));
    auditMap.put(OzoneConsts.TABLE_PARTITIONS, String.valueOf(this.partitions));
    auditMap.put(OzoneConsts.USED_CAPACITY_IN_BYTES, String.valueOf(this.usedInBytes));
    auditMap.put(OzoneConsts.QUOTA_IN_BYTES, String.valueOf(this.quotaInBytes));
    auditMap.put(OzoneConsts.QUOTA_IN_NAMESPACE, String.valueOf(this.quotaInNamespace));
    return auditMap;
  }

  /**
   * Return a new copy of the object.
   */
  public OmTableInfo copyObject() {
    Builder builder = toBuilder();
    return builder.build();
  }

  public Builder toBuilder() {
    return new Builder()
        .setDatabaseName(databaseName)
        .setTableName(tableName)
        .setStorageType(storageType)
        .setIsVersionEnabled(isVersionEnabled)
        .setCreationTime(creationTime)
        .setModificationTime(modificationTime)
        .setObjectID(objectID)
        .setUpdateID(updateID)
        .addAllMetadata(metadata)
        .setColumns(columns)
        .setStorageEngine(storageEngine)
        .setNumReplicas(numReplicas)
        .setPartitions(partitions)
        .setColumnKey(columnKey)
        .setDistributedKey(distributedKeyProto)
        .setUsedInBytes(usedInBytes);
  }

    /**
   * Builder for OmBucketInfo.
   */
  public static class Builder {
    private String databaseName;
    private String tableName;
    private List<ColumnSchema> columns;
    private ColumnKey columnKey;
    private StorageEngineProto storageEngine;
    private Boolean isVersionEnabled;
    private StorageType storageType;
    private long creationTime;
    private long modificationTime;
    private int numReplicas;
    private PartitionsProto partitions;
    private DistributedKeyProto distributedKey;
    private long usedInBytes;
    private long quotaInBytes;
    private long quotaInNamespace;
    private long objectID;
    private long updateID;
    private Map<String, String> metadata;

    public Builder() {
      //Default values
      this.isVersionEnabled = false;
      this.storageType = StorageType.DISK;
      this.storageEngine = StorageEngineProto.LSTORE;
      this.metadata = new HashMap<>();
      this.usedInBytes = OzoneConsts.USED_CAPACITY_IN_BYTES_RESET;
    }

    public Builder setDatabaseName(String databaseName) {
      this.databaseName = databaseName;
      return this;
    }

    public Builder setTableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    public Builder setIsVersionEnabled(Boolean versionFlag) {
      this.isVersionEnabled = versionFlag;
      return this;
    }

    public Builder setStorageType(StorageType storage) {
      this.storageType = storage;
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

    public Builder setColumnKey(ColumnKey columnKey) {
      this.columnKey = columnKey;
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

    public Builder setColumns(List<ColumnSchema> columns) {
      this.columns = columns;
      return this;
    }

    public Builder setStorageEngine(StorageEngineProto storageEngine) {
      this.storageEngine = storageEngine;
      return this;
    }

    public Builder setNumReplicas(int numReplicas) {
      this.numReplicas = numReplicas;
      return this;
    }

    public Builder setPartitions(PartitionsProto partitions) {
      this.partitions = partitions;
      return this;
    }

    public Builder setUsedInBytes(long usedBytes) {
      this.usedInBytes = usedInBytes;
      return this;
    }

    public Builder setDistributedKey(DistributedKeyProto distributedKey) {
      this.distributedKey = distributedKey;
      return this;
    }

    public Builder setQuotaInBytes(long quotaInBytes) {
        this.quotaInBytes = quotaInBytes;
        return this;
    }

    public Builder setQuotaInNamespace(long quotaInNamespace) {
      this.quotaInNamespace = quotaInNamespace;
      return this;
    }

    /**
     * Constructs the OmBucketInfo.
     * @return instance of OmBucketInfo.
     */
    public OmTableInfo build() {
      Preconditions.checkNotNull(databaseName);
      Preconditions.checkNotNull(tableName);
      Preconditions.checkArgument(columns.size() > 0);
      Preconditions.checkNotNull(isVersionEnabled);
      Preconditions.checkNotNull(storageType);
      Preconditions.checkNotNull(numReplicas);
      Preconditions.checkNotNull(columnKey);

      return new OmTableInfo(databaseName, tableName, isVersionEnabled,
          storageType, columns, columnKey, storageEngine, numReplicas, partitions, distributedKey,
          creationTime, modificationTime, objectID, updateID,
          metadata, usedInBytes, quotaInBytes, quotaInNamespace);
    }
  }

  /**
   * Creates BucketInfo protobuf from OmBucketInfo.
   */
  public TableInfo getProtobuf() {
    List<ColumnSchemaProto> columnSchemaProtos = columns.stream()
            .map(proto -> ColumnSchema.toProtobuf(proto))
            .collect(toList());

    TableInfo.Builder bib =  TableInfo.newBuilder()
        .setDatabaseName(databaseName)
        .setTableName(tableName)
        .setIsVersionEnabled(isVersionEnabled)
        .setStorageType(storageType.toProto())
        .setCreationTime(creationTime)
        .setModificationTime(modificationTime)
        .setObjectID(objectID)
        .setUpdateID(updateID)
        .addAllColumns(columnSchemaProtos)
        .setStorageEngine(storageEngine)
        .setNumReplicas(numReplicas)
        .setPartitions(partitions)
        .setColumnKey(columnKey.toProtobuf())
        .setDistributedKey(distributedKeyProto)
        .setUsedCapacityInBytes(usedInBytes);
    return bib.build();
  }

  /**
   * Parses TableInfo protobuf and creates OmTableInfo.
   * @param tableInfo
   * @return instance of OmTableInfo
   */
  public static OmTableInfo getFromProtobuf(TableInfo tableInfo) {
    List<ColumnSchema> columnSchemas = tableInfo.getColumnsList().stream()
        .map(columnSchemaProto -> ColumnSchema.fromProtobuf(columnSchemaProto))
        .collect(toList());
    OmTableInfo.Builder obib = OmTableInfo.newBuilder()
        .setDatabaseName(tableInfo.getDatabaseName())
        .setTableName(tableInfo.getTableName())
        .setIsVersionEnabled(tableInfo.getIsVersionEnabled())
        .setStorageType(StorageType.valueOf(tableInfo.getStorageType()))
        .setCreationTime(tableInfo.getCreationTime())
        .setModificationTime(tableInfo.getModificationTime())
        .setColumns(columnSchemas)
        .setStorageEngine(tableInfo.getStorageEngine())
        .setNumReplicas(tableInfo.getNumReplicas())
        .setPartitions(tableInfo.getPartitions())
        .setColumnKey(ColumnKey.fromProtobuf(tableInfo.getColumnKey()))
        .setDistributedKey(tableInfo.getDistributedKey())
        .setUsedInBytes(tableInfo.getUsedCapacityInBytes());

    if (tableInfo.hasObjectID()) {
      obib.setObjectID(tableInfo.getObjectID());
    }
    if (tableInfo.hasUpdateID()) {
      obib.setUpdateID(tableInfo.getUpdateID());
    }
    if (tableInfo.getMetadataList() != null) {
      obib.addAllMetadata(KeyValueUtil
          .getFromProtobuf(tableInfo.getMetadataList()));
    }
    return obib.build();
  }

  @Override
  public String getObjectInfo() {
    return "OmTableInfo{" +
        "databaseName='" + databaseName + '\'' +
        ", tableName='" + tableName + '\'' +
        ", columns=" + columns +
        ", storageEngine=" + storageEngine +
        ", isVersionEnabled=" + isVersionEnabled +
        ", storageType=" + storageType +
        ", creationTime=" + creationTime +
        ", modificationTime=" + modificationTime +
        ", numReplicas=" + numReplicas +
        ", partitions=" + partitions +
        ", distributedKey=" + distributedKeyProto +
        ", usedInBytes='" + usedInBytes + '\'' +
        ", quotaInNamespace='" + quotaInNamespace + '\'' +
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
    OmTableInfo that = (OmTableInfo) o;
    return creationTime == that.creationTime &&
        modificationTime == that.modificationTime &&
        databaseName.equals(that.databaseName) &&
        tableName.equals(that.tableName) &&
        Objects.equals(isVersionEnabled, that.isVersionEnabled) &&
        storageType == that.storageType &&
        objectID == that.objectID &&
        updateID == that.updateID &&
        columns == that.columns &&
        storageEngine == that.storageEngine &&
        numReplicas == that.numReplicas &&
        partitions == that.partitions &&
        distributedKeyProto == this.distributedKeyProto &&
        usedInBytes == that.usedInBytes &&
        quotaInNamespace == that.quotaInNamespace &&
        Objects.equals(metadata, that.metadata);
  }

  @Override
  public int hashCode() {
    return Objects.hash(databaseName, tableName);
  }

  @Override
  public String toString() {
    return "OmTableInfo{" +
        "databaseName='" + databaseName + '\'' +
        ", tableName='" + tableName + '\'' +
        ", columns=" + columns +
        ", columnKey=" + columnKey +
        ", storageEngine=" + storageEngine +
        ", isVersionEnabled=" + isVersionEnabled +
        ", storageType=" + storageType +
        ", creationTime=" + creationTime +
        ", modificationTime=" + modificationTime +
        ", numReplicas=" + numReplicas +
        ", partitions=" + partitions +
        ", distributedKey=" + distributedKeyProto +
        ", usedInBytes='" + usedInBytes + '\'' +
        ", quotaInNamespace='" + quotaInNamespace + '\'' +
        '}';
  }
}
