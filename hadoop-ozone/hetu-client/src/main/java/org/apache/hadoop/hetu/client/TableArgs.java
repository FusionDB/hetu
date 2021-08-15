/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hetu.client;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hetu.hm.meta.table.Schema;
import org.apache.hadoop.hetu.hm.meta.table.StorageEngine;
import org.apache.hadoop.ozone.OzoneConsts;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * This class encapsulates the arguments that are
 * required for creating a table.
 */
public final class TableArgs {
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
   * Type of storage engine to be used for this table.
   * [LSTORE, CSTORE]
   */
  private StorageEngine storageEngine;
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

  private Map<String, String> metadata = new HashMap<>();

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
  private TableArgs(String databaseName, String tableName, Schema schema,
                      Boolean isVersionEnabled, StorageType storageType,
                      StorageEngine storageEngine, int numReplicas, int buckets,
                      Map<String, String> metadata, long usedBytes,
                      long quotaInBytes, int usedBucket, int quotaInBucket) {
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.schema = schema;
    this.isVersionEnabled = isVersionEnabled;
    this.storageType = storageType;
    this.storageEngine = storageEngine;
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

  public StorageEngine getStorageEngine() {
    return storageEngine;
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

  public Map<String, String> getMetadata() {
    return metadata;
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
   * Returns new builder class that builds a TableArgs.
   * @return Builder
   */
  public static TableArgs.Builder newBuilder() {
    return new TableArgs.Builder();
  }

  /**
   * Builder for TableArgs.
   */
  public static class Builder {
    private String databaseName;
    private String tableName;
    private Schema schema;
    private Boolean isVersionEnabled;
    private StorageType storageType;
    private StorageEngine storageEngine;
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

    public TableArgs.Builder setDatabaseName(String databaseName) {
      this.databaseName = databaseName;
      return this;
    }

    public TableArgs.Builder setTableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    public TableArgs.Builder setIsVersionEnabled(Boolean versionFlag) {
      this.isVersionEnabled = versionFlag;
      return this;
    }

    public TableArgs.Builder addMetadata(Map<String, String> metadataMap) {
      this.metadata = metadataMap;
      return this;
    }

    public TableArgs.Builder setStorageType(StorageType storage) {
      this.storageType = storage;
      return this;
    }

    public TableArgs.Builder setStorageEngine(StorageEngine storageEngine) {
      this.storageEngine = storageEngine;
      return this;
    }

    public TableArgs.Builder setNumReplicas(int numReplicas) {
      this.numReplicas = numReplicas;
      return this;
    }

    public TableArgs.Builder setSchema(Schema schema) {
      this.schema = schema;
      return this;
    }

    public TableArgs.Builder setUsedBytes(long usedBytes) {
      this.usedBytes = usedBytes;
      return this;
    }

    public TableArgs.Builder setQuotaInBytes(long quotaInBytes) {
      this.quotaInBytes = quotaInBytes;
      return this;
    }

    public TableArgs.Builder setBuckets(int buckets) {
      this.buckets = buckets;
      return this;
    }

    public TableArgs.Builder setUsedBucket(int usedBucket) {
      this.usedBucket = usedBucket;
      return this;
    }

    public TableArgs.Builder setQuotaInBucket(int quotaInBucket) {
      this.quotaInBucket = quotaInBucket;
      return this;
    }

    /**
     * Constructs the TableArgs.
     * @return instance of TableArgs.
     */
    public TableArgs build() {
      Preconditions.checkNotNull(databaseName);
      Preconditions.checkNotNull(tableName);
      Preconditions.checkNotNull(schema);
      Preconditions.checkNotNull(numReplicas);
      Preconditions.checkNotNull(buckets);
      return new TableArgs(databaseName, tableName, schema, isVersionEnabled,
              storageType, storageEngine, numReplicas, buckets, metadata, usedBytes, quotaInBytes,
              usedBucket, quotaInBucket);
    }

  }
}
