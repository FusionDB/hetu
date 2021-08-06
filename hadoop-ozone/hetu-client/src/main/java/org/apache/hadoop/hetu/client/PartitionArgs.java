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

import java.util.HashMap;
import java.util.Map;

/**
 * This class encapsulates the arguments that are
 * required for creating a table.
 */
public final class PartitionArgs {

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
   * Custom key/value metadata.
   */
  private Map<String, String> metadata;

  /**
   * Partition encryption tablet name.
   */
  private String partitionEncryptionTablet;
  private final String databaseName;
  private final String tableName;
  private final String partitionName;
  private final String partitionValue;

  private long sizeInBytes;

  private int numReplicas;

  /**
   * Private constructor, constructed via builder.
   * @param versioning Table version flag.
   * @param storageType Storage type to be used.
   * @param metadata map of table metadata
   * @param partitionEncryptionTablet partition encryption tablet name
   * @param databaseName
   * @param tableName
   * @param partitionName
   * @param partitionValue
   * @param sizeInBytes Table quota in bytes.
   */
  @SuppressWarnings("parameternumber")
  private PartitionArgs(Boolean versioning, StorageType storageType,
                        Map<String, String> metadata, String partitionEncryptionTablet,
                        String databaseName, String tableName,
                        String partitionName, String partitionValue,
                        long sizeInBytes) {
    this.versioning = versioning;
    this.storageType = storageType;
    this.metadata = metadata;
    this.partitionEncryptionTablet = partitionEncryptionTablet;
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.partitionName = partitionName;
    this.partitionValue = partitionValue;
    this.sizeInBytes = sizeInBytes;
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
   * Returns the partition encryption tablet name.
   * @return partition encryption tablet
   */
  public String getEncryptionTablet() {
    return partitionEncryptionTablet;
  }

  /**
   * Returns new builder class that builds a OmPartitionInfo.
   *
   * @return Builder
   */
  public static PartitionArgs.Builder newBuilder() {
    return new PartitionArgs.Builder();
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

  /**
   * Returns Table Size in bytes.
   * @return sizeInBytes.
   */
  public long getSizeInBytes() {
    return sizeInBytes;
  }

  public String getPartitionValue() {
    return partitionValue;
  }

  public int getNumReplicas() {
    return numReplicas;
  }

  /**
   * Builder for OmPartitionInfo.
   */
  public static class Builder {
    private Boolean versioning;
    private StorageType storageType;
    private Map<String, String> metadata;
    private String partitionEncryptionTablet;
    private String databaseName;
    private String tableName;
    private String partitionName;
    private String partitionValue;
    private long sizeInBytes;

    public Builder() {
      metadata = new HashMap<>();
      sizeInBytes = OzoneConsts.QUOTA_RESET;
    }

    public PartitionArgs.Builder setVersioning(Boolean versionFlag) {
      this.versioning = versionFlag;
      return this;
    }

    public PartitionArgs.Builder setStorageType(StorageType storage) {
      this.storageType = storage;
      return this;
    }

    public PartitionArgs.Builder addMetadata(String key, String value) {
      this.metadata.put(key, value);
      return this;
    }

    public PartitionArgs.Builder setTableEncryptionTable(String pet) {
      this.partitionEncryptionTablet = pet;
      return this;
    }

    public PartitionArgs.Builder setDatabaseName(String database) {
      this.databaseName = database;
      return this;
    }

    public PartitionArgs.Builder setTableName(String table) {
      this.tableName = table;
      return this;
    }

    public PartitionArgs.Builder setPartitionName(String partition) {
      this.partitionName = partition;
      return this;
    }

    public PartitionArgs.Builder setPartitionValue(String partitionValue) {
      this.partitionValue = partitionValue;
      return this;
    }


    /**
     * Constructs the PartitionArgs.
     * @return instance of PartitionArgs.
     */
    public PartitionArgs build() {
      return new PartitionArgs(versioning, storageType, metadata,
          partitionEncryptionTablet, databaseName, tableName, partitionName,
          partitionValue, sizeInBytes);
    }
  }
}
