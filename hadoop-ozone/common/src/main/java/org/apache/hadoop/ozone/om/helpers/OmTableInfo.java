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
import org.apache.hadoop.hetu.hm.meta.table.Schema;
import org.apache.hadoop.hetu.hm.meta.table.StorageEngine;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.Auditable;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TableInfo;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;


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
  private Schema schema;

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
   * Type of storage to be used for this table.
   * [LSTORE, CSTORE]
   */
  private StorageEngine storageEngine;
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
   * Table num of bucket: default 8
   */
  private int buckets;
  /**
   * Table of usedBytes
   */
  private long usedBytes;
  /**
   * Table of quotaBytes
   */
  private long quotaInBytes;
  /**
   * Table of usedBucket
   */
  private int usedBucket;
  /**
   * Table of quotaInBucket
   */
  public int quotaInBucket;

  /**
   * Private constructor, constructed via builder.
   * @param databaseName - Database name.
   * @param tableName - Table name.
   * @param isVersionEnabled - Table version flag.
   * @param storageType - Storage type to be used.
   * @param creationTime - Table creation time.
   * @param modificationTime - Table modification time.
   * @param metadata - metadata.
   */
  @SuppressWarnings("checkstyle:ParameterNumber")
  private OmTableInfo(String databaseName,
                      String tableName,
                      Schema schema,
                      boolean isVersionEnabled,
                      StorageType storageType,
                      StorageEngine storageEngine,
                      int numReplicas,
                      int buckets,
                      long creationTime,
                      long modificationTime,
                      long objectID,
                      long updateID,
                      Map<String, String> metadata,
                      long usedBytes,
                      long quotaInBytes,
                      int usedBucket,
                      int quotaInBcuket) {
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.schema = schema;
    this.isVersionEnabled = isVersionEnabled;
    this.storageType = storageType;
    this.storageEngine = storageEngine;
    this.creationTime = creationTime;
    this.modificationTime = modificationTime;
    this.objectID = objectID;
    this.updateID = updateID;
    this.metadata = metadata;
    this.numReplicas = numReplicas;
    this.buckets = buckets;
    this.usedBytes = usedBytes;
    this.quotaInBytes = quotaInBytes;
    this.usedBucket = usedBucket;
    this.quotaInBucket = quotaInBcuket;
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
   * @return Schema
   */
  public Schema getSchema() {
    return schema;
  }

  /**
   * Returns StorageEngine format
   * @return
   */
  public StorageEngine getStorageEngine() {
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
   * Returns table bucket is data shard number
   * @return
   */
  public int getBuckets() {
    return buckets;
  }

  /**
   * Returns table used capacity in bytes
   * @return
   */
  public long getUsedBytes() {
    return usedBytes;
  }

  /**
   * Returns Quota in Bytes.
   * @return long, Quota in bytes.
   */
  public long getQuotaInBytes() {
    return quotaInBytes;
  }

  /**
   * Retruns table used count in buckets
   * @return
   */
  public int getUsedBucket() {
    return usedBucket;
  }

  /**
   * Returns Quota in Bucket Number
   * @return
   */
  public int getQuotaInBucket() {
    return quotaInBucket;
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
    auditMap.put(OzoneConsts.TABLE_SCHEMA, schema.toString());
    auditMap.put(OzoneConsts.STORAGE_ENGINE, String.valueOf(this.storageEngine));
    auditMap.put(OzoneConsts.NUM_REPLICAS, String.valueOf(this.numReplicas));
    auditMap.put(OzoneConsts.USED_IN_BYTES, String.valueOf(this.usedBytes));
    auditMap.put(OzoneConsts.QUOTA_IN_BYTES, String.valueOf(this.quotaInBytes));
    auditMap.put(OzoneConsts.USED_IN_BUCKET, String.valueOf(this.usedBucket));
    auditMap.put(OzoneConsts.QUOTA_IN_BUCKET, String.valueOf(this.quotaInBucket));
    auditMap.put(OzoneConsts.BUCKETS, String.valueOf(this.buckets));
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
        .setSchema(schema)
        .setStorageType(storageType)
        .setIsVersionEnabled(isVersionEnabled)
        .setCreationTime(creationTime)
        .setModificationTime(modificationTime)
        .setObjectID(objectID)
        .setUpdateID(updateID)
        .addAllMetadata(metadata)
        .setStorageEngine(storageEngine)
        .setNumReplicas(numReplicas)
        .setBuckets(buckets)
        .setUsedBytes(usedBytes)
        .setQuotaInBytes(quotaInBytes)
        .setUsedBucket(usedBucket)
        .setQuotaInBucket(quotaInBucket);
  }

    /**
   * Builder for OmBucketInfo.
   */
  public static class Builder {
    private String databaseName;
    private String tableName;
    private Schema schema;
    private StorageEngine storageEngine;
    private Boolean isVersionEnabled;
    private StorageType storageType;
    private long creationTime;
    private long modificationTime;
    private int numReplicas;
    private int buckets;
    private long usedBytes;
    private long quotaInBytes;
    private int usedBucket;
    private int quotaInBucket;
    private long objectID;
    private long updateID;
    private Map<String, String> metadata;

    public Builder() {
      //Default values
      this.isVersionEnabled = false;
      this.storageType = StorageType.DISK;
      this.storageEngine = StorageEngine.LSTORE;
      this.metadata = new HashMap<>();
      this.usedBytes = OzoneConsts.USED_IN_BYTES_RESET;
      this.quotaInBytes = OzoneConsts.HETU_QUOTA_RESET;
      this.usedBucket = OzoneConsts.USED_IN_BUCKET_RESET;
      this.quotaInBucket = OzoneConsts.HETU_BUCKET_QUOTA_RESET;
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

    public Builder setSchema(Schema schema) {
      this.schema = schema;
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

    public Builder setStorageEngine(StorageEngine storageEngine) {
      this.storageEngine = storageEngine;
      return this;
    }

    public Builder setNumReplicas(int numReplicas) {
      this.numReplicas = numReplicas;
      return this;
    }

    public Builder setBuckets(int buckets) {
      this.buckets = buckets;
      return this;
    }

    public Builder setUsedBytes(long usedBytes) {
      this.usedBytes = usedBytes;
      return this;
    }

    public Builder setQuotaInBytes(long quotaInBytes) {
        this.quotaInBytes = quotaInBytes;
        return this;
    }

    public Builder setUsedBucket(int usedBucket) {
      this.usedBucket = usedBucket;
      return this;
    }
    public Builder setQuotaInBucket(int quotaInBucket) {
      this.quotaInBucket = quotaInBucket;
      return this;
    }

    /**
     * Constructs the OmBucketInfo.
     * @return instance of OmBucketInfo.
     */
    public OmTableInfo build() {
      Preconditions.checkNotNull(databaseName);
      Preconditions.checkNotNull(tableName);
      Preconditions.checkNotNull(schema);
      Preconditions.checkNotNull(isVersionEnabled);
      Preconditions.checkNotNull(storageType);
      Preconditions.checkNotNull(numReplicas);
      Preconditions.checkNotNull(buckets);

      return new OmTableInfo(databaseName, tableName, schema, isVersionEnabled,
          storageType, storageEngine, numReplicas, buckets,
          creationTime, modificationTime, objectID, updateID,
          metadata, usedBytes, quotaInBytes, usedBucket, quotaInBucket);
    }
  }

  /**
   * Creates TableInfo protobuf from OmTableInfo.
   */
  public TableInfo getProtobuf() {
    TableInfo.Builder bib =  TableInfo.newBuilder()
        .setDatabaseName(databaseName)
        .setTableName(tableName)
        .setSchema(schema.toProtobuf())
        .setIsVersionEnabled(isVersionEnabled)
        .setStorageType(storageType.toProto())
        .setCreationTime(creationTime)
        .setModificationTime(modificationTime)
        .setObjectID(objectID)
        .setUpdateID(updateID)
        .setStorageEngine(storageEngine.toProto())
        .setNumReplicas(numReplicas)
        .setBuckets(buckets)
        .addAllMetadata(KeyValueUtil.toProtobuf(metadata))
        .setUsedBytes(usedBytes)
        .setQuotaInBytes(quotaInBytes)
        .setQuotaInBucket(quotaInBucket)
        .setUsedBucket(usedBucket);
    return bib.build();
  }

  /**
   * Parses TableInfo protobuf and creates OmTableInfo.
   * @param tableInfo
   * @return instance of OmTableInfo
   */
  public static OmTableInfo getFromProtobuf(TableInfo tableInfo) {
    OmTableInfo.Builder obib = OmTableInfo.newBuilder()
        .setDatabaseName(tableInfo.getDatabaseName())
        .setTableName(tableInfo.getTableName())
        .setSchema(Schema.fromProtobuf(tableInfo.getSchema()))
        .setIsVersionEnabled(tableInfo.getIsVersionEnabled())
        .setStorageType(StorageType.valueOf(tableInfo.getStorageType()))
        .setCreationTime(tableInfo.getCreationTime())
        .setModificationTime(tableInfo.getModificationTime())
        .setStorageEngine(StorageEngine.valueOf(tableInfo.getStorageEngine()))
        .setNumReplicas(tableInfo.getNumReplicas())
        .setBuckets(tableInfo.getBuckets())
        .setUsedBytes(tableInfo.getUsedBytes())
        .setQuotaInBytes(tableInfo.getQuotaInBytes())
        .setUsedBucket(tableInfo.getUsedBucket())
        .setQuotaInBucket(tableInfo.getQuotaInBucket());

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
            ", schema=" + schema +
            ", isVersionEnabled=" + isVersionEnabled +
            ", storageType=" + storageType +
            ", storageEngine=" + storageEngine +
            ", creationTime=" + creationTime +
            ", modificationTime=" + modificationTime +
            ", numReplicas=" + numReplicas +
            ", buckets=" + buckets +
            ", usedBytes=" + usedBytes +
            ", quotaInBytes=" + quotaInBytes +
            ", usedBucket=" + usedBucket +
            ", quotaInBucket=" + quotaInBucket +
            ", objectID=" + objectID +
            ", updateID=" + updateID +
            ", metadata=" + metadata +
            '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    OmTableInfo that = (OmTableInfo) o;
    return creationTime == that.creationTime &&
            modificationTime == that.modificationTime &&
            numReplicas == that.numReplicas &&
            buckets == that.buckets &&
            usedBytes == that.usedBytes &&
            quotaInBytes == that.quotaInBytes &&
            usedBucket == that.usedBucket &&
            quotaInBucket == that.quotaInBucket &&
            databaseName.equals(that.databaseName) &&
            tableName.equals(that.tableName) &&
            schema.equals(that.schema) &&
            isVersionEnabled.equals(that.isVersionEnabled) &&
            storageType == that.storageType &&
            storageEngine == that.storageEngine;
  }

  @Override
  public int hashCode() {
    return Objects.hash(databaseName, tableName, schema,
            isVersionEnabled, storageType, storageEngine,
            creationTime, modificationTime, numReplicas,
            buckets, usedBytes, quotaInBytes, usedBucket,
            quotaInBucket);
  }

  @Override
  public String toString() {
    return "OmTableInfo{" +
            "databaseName='" + databaseName + '\'' +
            ", tableName='" + tableName + '\'' +
            ", schema=" + schema +
            ", isVersionEnabled=" + isVersionEnabled +
            ", storageType=" + storageType +
            ", storageEngine=" + storageEngine +
            ", creationTime=" + creationTime +
            ", modificationTime=" + modificationTime +
            ", numReplicas=" + numReplicas +
            ", buckets=" + buckets +
            ", usedBytes=" + usedBytes +
            ", quotaInBytes=" + quotaInBytes +
            ", usedBucket=" + usedBucket +
            ", quotaInBucket=" + quotaInBucket +
            ", objectID=" + objectID +
            ", updateID=" + updateID +
            ", metadata=" + metadata +
            '}';
  }
}
