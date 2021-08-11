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

import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.hetu.hm.meta.table.ColumnKey;
import org.apache.hadoop.hetu.hm.meta.table.ColumnSchema;
import org.apache.hadoop.hetu.hm.meta.table.StorageEngine;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TableInfo.DistributedKeyProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TableInfo.PartitionsProto;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class encapsulates the arguments that are
 * required for creating a table.
 */
public final class TableArgs {
  /**
   * Name of the database in which the table belongs to.
   */
  private String databaseName;
  /**
   * Name of the table.
   */
  private String tableName;
  /**
   * Table Version flag.
   */
  private Boolean versioning;
  /**
   * Type of storage to be used for this table.
   * [RAM_DISK, SSD, DISK, ARCHIVE]
   */
  private StorageType storageType;
  /**
   * Engine of storage to be used for this table.
   * [LSTORE, CSTORE]
   */
  private StorageEngine storageEngine;

  /**
   * schema of the table
   */
  private List<ColumnSchema> columns;

  /**
   * Table of column key
   */
  private ColumnKey columnKey;

  /**
   * Table of distributeKey
   */
  private DistributedKeyProto distributedKeyProto;
  /**
   * Table of partitions
   */
  private PartitionsProto partitions;
  /**
   * Table num replicas
   */
  private int numReplicas;
  /**
   * Custom key/value metadata.
   */
  private Map<String, String> metadata;

  private long quotaInBytes;
  private long quotaInNamespace;

  /**
   * Private constructor, constructed via builder.
   * @param versioning Table version flag.
   * @param storageType Storage type to be used.
   * @param metadata map of table metadata
   * @param quotaInBytes Table quota in bytes.
   * @param quotaInNamespace Table quota in counts.
   */
  @SuppressWarnings("parameternumber")
  private TableArgs(Boolean versioning, StorageType storageType, StorageEngine storageEngine,
                    Map<String, String> metadata, List<ColumnSchema> columns,
                    String databaseName, String tableName, int numReplicas,
                    ColumnKey columnKey, DistributedKeyProto distributedKeyProto,
                    PartitionsProto partitions, long quotaInBytes,
                    long quotaInNamespace) {
    this.versioning = versioning;
    this.storageType = storageType;
    this.metadata = metadata;
    this.columns = columns;
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.numReplicas = numReplicas;
    this.storageEngine = storageEngine;
    this.columnKey = columnKey;
    this.distributedKeyProto = distributedKeyProto;
    this.partitions = partitions;
    this.quotaInBytes = quotaInBytes;
    this.quotaInNamespace = quotaInNamespace;
  }

  /**
   * Returns true if table version is enabled, else false.
   * @return isVersionEnabled
   */
  public Boolean getVersioning() {
    return versioning;
  }

  /**
   * Returns the type of storage to be used.
   * @return StorageType
   */
  public StorageType getStorageType() {
    return storageType;
  }

  /**
   * Custom metadata for the tables.
   *
   * @return key value map
   */
  public Map<String, String> getMetadata() {
    return metadata;
  }

  /**
   * Returns new builder class that builds a OmTabletInfo.
   *
   * @return Builder
   */
  public static TableArgs.Builder newBuilder() {
    return new TableArgs.Builder();
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public String getTableName() {
    return tableName;
  }

  public List<ColumnSchema> getColumns() {
    return columns;
  }

  public int getNumReplicas() {
    return numReplicas;
  }

  public ColumnKey getColumnKey() {
    return columnKey;
  }

  public DistributedKeyProto getDistributedKeyProto() {
    return distributedKeyProto;
  }

  public PartitionsProto getPartitions() {
    return partitions;
  }

  public StorageEngine getStorageEngine() {
    return storageEngine;
  }

  /**
   * Returns Table Quota in bytes.
   * @return quotaInBytes.
   */
  public long getQuotaInBytes() {
    return quotaInBytes;
  }

  /**
   * Returns Table Quota in tablet counts.
   * @return quotaInNamespace.
   */
  public long getQuotaInNamespace() {
    return quotaInNamespace;
  }

  /**
   * Builder for OmTableInfo.
   */
  public static class Builder {
    private String databaseName;
    private String tableName;
    private Boolean versioning;
    private StorageType storageType;
    private StorageEngine storageEngine;
    private Map<String, String> metadata;
    private List<ColumnSchema> columns;
    private ColumnKey columnKey;
    private DistributedKeyProto distributedKeyProto;
    private PartitionsProto partitions;
    private int numReplicas;
    private long quotaInBytes;
    private long quotaInNamespace;

    public Builder() {
      metadata = new HashMap<>();
      quotaInBytes = OzoneConsts.QUOTA_RESET;
      quotaInNamespace = OzoneConsts.QUOTA_RESET;
    }

    public TableArgs.Builder setVersioning(Boolean versionFlag) {
      this.versioning = versionFlag;
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

    public TableArgs.Builder addMetadata(String key, String value) {
      this.metadata.put(key, value);
      return this;
    }

    public TableArgs.Builder setDatabaseName(String databaseName) {
      this.databaseName = databaseName;
      return this;
    }

    public TableArgs.Builder setTableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    public TableArgs.Builder setColumns(List<ColumnSchema> columns) {
      this.columns = columns;
      return this;
    }

    public TableArgs.Builder setColumnKey(ColumnKey columnKey) {
      this.columnKey = columnKey;
      return this;
    }

    public TableArgs.Builder setDistributedKeyProto(DistributedKeyProto distributedKeyProto) {
      this.distributedKeyProto = distributedKeyProto;
      return this;
    }

    public TableArgs.Builder setPartitions(PartitionsProto partitions) {
      this.partitions = partitions;
      return this;
    }

    public TableArgs.Builder setNumReplicas(int numReplicas) {
      this.numReplicas = numReplicas;
      return this;
    }

    public TableArgs.Builder setQuotaInBytes(long quota) {
      quotaInBytes = quota;
      return this;
    }

    public TableArgs.Builder setQuotaInNamespace(long quota) {
      quotaInNamespace = quota;
      return this;
    }

    /**
     * Constructs the TableArgs.
     * @return instance of TableArgs.
     */
    public TableArgs build() {
      return new TableArgs(versioning, storageType, storageEngine, metadata,
          columns, databaseName, tableName, numReplicas, columnKey, distributedKeyProto,
          partitions, quotaInBytes, quotaInNamespace);
    }
  }
}
