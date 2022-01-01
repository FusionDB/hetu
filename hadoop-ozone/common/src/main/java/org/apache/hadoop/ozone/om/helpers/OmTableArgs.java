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
import org.apache.hadoop.hetu.photon.meta.schema.Schema;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.Auditable;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TableArgs;

import java.util.LinkedHashMap;
import java.util.Map;


/**
 * A class that encapsulates Table Arguments.
 */
public final class OmTableArgs extends WithMetadata implements Auditable {
  /**
   * Name of the database in which the table belongs to.
   */
  private final String databaseName;
  /**
   * Name of the table.
   */
  private final String tableName;
  /**
   * schema of the table
   */
  private final Schema schema;
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
   * Table num buckets
   */
  private int buckets;
  /**
   * Table num replicas
   */
  private int numReplicas;
  /**
   * Table used bytes
   */
  private long usedBytes;
  /**
   * Table quota in bytes
   */
  private long quotaInBytes;
  /**
   * Table used num of buckets
   */
  private int usedBucket;
  /**
   * Table quota in bucket.
   */
  private int quotaInBucket;

  /**
   * Private constructor, constructed via builder.
   * @param databaseName - Database name.
   * @param tableName - Table name.
   * @param schema - Table schema.
   * @param isVersionEnabled - Table version flag.
   * @param storageType - Storage type to be used.
   * @param usedBytes - Table used in bytes.
   * @param usedBucket - Table used bucket count
   */
  private OmTableArgs(String databaseName, String tableName, Schema schema,
                      Boolean isVersionEnabled, StorageType storageType,
                       int numReplicas, int buckets,
                      Map<String, String> metadata, long usedBytes,
                      long quotaInBytes, int usedBucket, int quotaInBucket) {
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.schema = schema;
    this.isVersionEnabled = isVersionEnabled;
    this.storageType = storageType;
    this.metadata = metadata;
    this.numReplicas = numReplicas;
    this.buckets = buckets;
    this.usedBytes = usedBytes;
    this.quotaInBytes = quotaInBytes;
    this.usedBucket = usedBucket;
    this.quotaInBucket = quotaInBucket;
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
   * Returns Table used capacity in bytes.
   * @return usedInBytes.
   */
  public long getUsedBytes() {
    return usedBytes;
  }

  /**
   * Returns Table used count in bucket.
   * @return
   */
  public int getUsedBucket() {
    return usedBucket;
  }

  /**
   * Returns Table quota in bytes.
   * @return
   */
  public long getQuotaInBytes() {
    return quotaInBytes;
  }

  /**
   * Returns Table quota in bucket count.
   * @return
   */
  public int getQuotaInBucket() {
    return quotaInBucket;
  }

  /**
   * Returns Table Schema
   * @return
   */
  public Schema getSchema() {
    return schema;
  }

  /**
   * Returns Table Num Replicas
   * @return
   */
  public int getNumReplicas() {
    return numReplicas;
  }

  /**
   * Returns Table num buckets
   * @return
   */
  public int getBuckets() {
    return buckets;
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
    auditMap.put(OzoneConsts.TABLE_SCHEMA, this.schema.toString());
    auditMap.put(OzoneConsts.GDPR_FLAG,
        this.metadata.get(OzoneConsts.GDPR_FLAG));
    auditMap.put(OzoneConsts.IS_VERSION_ENABLED,
                String.valueOf(this.isVersionEnabled));
    if(this.storageType != null) {
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
    private Schema schema;
    private Boolean isVersionEnabled;
    private StorageType storageType;
    private Map<String, String> metadata;
    private int numReplicas;
    private int buckets;
    private long usedBytes;
    private long quotaInBytes;
    private int usedBucket;
    private int quotaInBucket;

    /**
     * Constructs a builder.
     */
    public Builder() {
      usedBytes = OzoneConsts.USED_IN_BYTES_RESET;
      usedBucket = OzoneConsts.USED_IN_BUCKET_RESET;
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

    public Builder setSchema(Schema schema) {
      this.schema = schema;
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

    public Builder setBuckets(int buckets) {
      this.buckets = buckets;
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
     * Constructs the OmTableArgs.
     * @return instance of OmTableArgs.
     */
    public OmTableArgs build() {
      Preconditions.checkNotNull(databaseName);
      Preconditions.checkNotNull(tableName);
      Preconditions.checkNotNull(schema);
      Preconditions.checkNotNull(numReplicas);
      Preconditions.checkNotNull(buckets);
      return new OmTableArgs(databaseName, tableName, schema, isVersionEnabled,
          storageType, numReplicas, buckets, metadata, usedBytes, quotaInBytes,
          usedBucket, quotaInBucket);
    }

  }

  /**
   * Creates TableArgs protobuf from OmTableArgs.
   */
  public TableArgs getProtobuf() {
    TableArgs.Builder builder = TableArgs.newBuilder();
    builder.setDatabaseName(databaseName)
        .setTableName(tableName)
        .setSchema(schema.toProtobuf())
        .setBuckets(buckets)
        .setNumReplicas(numReplicas);
    if(isVersionEnabled != null) {
      builder.setIsVersionEnabled(isVersionEnabled);
    }
    if(storageType != null) {
      builder.setStorageType(storageType.toProto());
    }
    if(usedBytes > 0 || usedBytes == OzoneConsts.USED_IN_BYTES_RESET) {
      builder.setUsedBytes(usedBytes);
    }
    if (usedBucket > 0 || usedBucket == OzoneConsts.USED_IN_BUCKET_RESET) {
      builder.setUsedBucket(usedBucket);
    }
    if(quotaInBytes > 0 || quotaInBytes == OzoneConsts.HETU_QUOTA_RESET) {
      builder.setQuotaInBytes(quotaInBytes);
    }
    if(quotaInBucket > 0 || quotaInBucket == OzoneConsts.HETU_BUCKET_QUOTA_RESET) {
      builder.setQuotaInBucket(quotaInBucket);
    }
    return builder.build();
  }

  /**
   * Parses TableInfo protobuf and creates OmTableArgs.
   * @param tableArgs
   * @return instance of OmTableArgs
   */
  public static OmTableArgs getFromProtobuf(TableArgs tableArgs) {
    return new OmTableArgs(tableArgs.getDatabaseName(),
        tableArgs.getTableName(),
        Schema.fromProtobuf(tableArgs.getSchema()),
        tableArgs.hasIsVersionEnabled() ?
        tableArgs.getIsVersionEnabled() : null,
        tableArgs.hasStorageType() ? StorageType.valueOf(
        tableArgs.getStorageType()) : null,
        tableArgs.getNumReplicas(),
        tableArgs.getBuckets(),
        KeyValueUtil.getFromProtobuf(tableArgs.getMetadataList()),
        tableArgs.getUsedBytes(), tableArgs.getQuotaInBytes(),
        tableArgs.getUsedBucket(), tableArgs.getQuotaInBucket());
  }
}
