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
public final class TableArgs {

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
   * Table encryption tablet name.
   */
  private String tableEncryptionTablet;
  private final String sourceDatabase;
  private final String sourceTable;

  private long quotaInBytes;
  private long quotaInNamespace;

  /**
   * Private constructor, constructed via builder.
   * @param versioning Table version flag.
   * @param storageType Storage type to be used.
   * @param metadata map of table metadata
   * @param tableEncryptionTablet table encryption partition/tablet name
   * @param sourceDatabase
   * @param sourceTable
   * @param quotaInBytes Table quota in bytes.
   * @param quotaInNamespace Table quota in counts.
   */
  @SuppressWarnings("parameternumber")
  private TableArgs(Boolean versioning, StorageType storageType,
                    Map<String, String> metadata, String tableEncryptionTablet,
                    String sourceDatabase, String sourceTable,
                    long quotaInBytes, long quotaInNamespace) {
    this.versioning = versioning;
    this.storageType = storageType;
    this.metadata = metadata;
    this.tableEncryptionTablet = tableEncryptionTablet;
    this.sourceDatabase = sourceDatabase;
    this.sourceTable = sourceTable;
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
   * Returns the table encryption partition/tablet name.
   * @return table encryption partition/tablet
   */
  public String getEncryptionTablet() {
    return tableEncryptionTablet;
  }

  /**
   * Returns new builder class that builds a OmTabletInfo.
   *
   * @return Builder
   */
  public static TableArgs.Builder newBuilder() {
    return new TableArgs.Builder();
  }

  public String getSourceDatabase() {
    return sourceDatabase;
  }

  public String getSourceTable() {
    return sourceTable;
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
    private Boolean versioning;
    private StorageType storageType;
    private Map<String, String> metadata;
    private String tableEncryptionTablet;
    private String sourceDatabase;
    private String sourceTable;
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

    public TableArgs.Builder addMetadata(String key, String value) {
      this.metadata.put(key, value);
      return this;
    }

    public TableArgs.Builder setTableEncryptionTable(String tet) {
      this.tableEncryptionTablet = tet;
      return this;
    }

    public TableArgs.Builder setSourceDatabase(String database) {
      sourceDatabase = database;
      return this;
    }

    public TableArgs.Builder setSourceTable(String table) {
      sourceTable = table;
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
      return new TableArgs(versioning, storageType, metadata,
          tableEncryptionTablet, sourceDatabase, sourceTable, quotaInBytes,
          quotaInNamespace);
    }
  }
}
