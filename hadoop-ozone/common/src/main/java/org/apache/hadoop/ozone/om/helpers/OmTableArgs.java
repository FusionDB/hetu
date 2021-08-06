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
import org.apache.hadoop.ozone.hm.meta.table.ColumnSchema;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TableArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ColumnSchemaProto;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;

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
   * Table Version flag.
   */
  private Boolean isVersionEnabled;
  /**
   * Type of storage to be used for this table.
   * [RAM_DISK, SSD, DISK, ARCHIVE]
   */
  private StorageType storageType;
  /**
   * schema of the table
   */
  private List<ColumnSchema> columns;

  /**
   * Table num replicas
   */
  private int numReplicas;

  private long usedCapacityInBytes;

  /**
   * Private constructor, constructed via builder.
   * @param databaseName - Database name.
   * @param tableName - Table name.
   * @param isVersionEnabled - Table version flag.
   * @param storageType - Storage type to be used.
   * @param usedCapacityInBytes Table quota in bytes.
   */
  private OmTableArgs(String databaseName, String tableName,
                      Boolean isVersionEnabled, StorageType storageType,
                      List<ColumnSchema> columns, int numReplicas,
                      Map<String, String> metadata, long usedCapacityInBytes) {
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.isVersionEnabled = isVersionEnabled;
    this.storageType = storageType;
    this.metadata = metadata;
    this.columns = columns;
    this.numReplicas = numReplicas;
    this.usedCapacityInBytes = usedCapacityInBytes;
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
   * @return usedCapacityInBytes.
   */
  public long getUsedCapacityInBytes() {
    return usedCapacityInBytes;
  }

  /**
   * Returns Table Schema
   * @return
   */
  public List<ColumnSchema> getColumns() {
    return columns;
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
    private Boolean isVersionEnabled;
    private StorageType storageType;
    private Map<String, String> metadata;
    private int numReplicas;
    private List<ColumnSchema> columns;
    private long usedCapacityInBytes;
    private long quotaInBytes;
    private long quotaInNamespace;

    /**
     * Constructs a builder.
     */
    public Builder() {
      usedCapacityInBytes = OzoneConsts.USED_CAPACITY_IN_BYTES_RESET;
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

    public Builder setColumns(List<ColumnSchema> columns) {
      this.columns = columns;
      return this;
    }

    public Builder setUsedCapacityInBytes(long usedCapacityInBytes) {
      this.usedCapacityInBytes = usedCapacityInBytes;
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
     * Constructs the OmTableArgs.
     * @return instance of OmTableArgs.
     */
    public OmTableArgs build() {
      Preconditions.checkNotNull(databaseName);
      Preconditions.checkNotNull(tableName);
      Preconditions.checkArgument(columns.size() > 0);
      Preconditions.checkNotNull(numReplicas);
      return new OmTableArgs(databaseName, tableName, isVersionEnabled,
          storageType, columns, numReplicas, metadata, usedCapacityInBytes);
    }

  }

  /**
   * Creates TableArgs protobuf from OmTableArgs.
   */
  public TableArgs getProtobuf() {
    TableArgs.Builder builder = TableArgs.newBuilder();
    List<ColumnSchemaProto> columnSchemaProtos = columns.stream()
            .map(columnSchema -> ColumnSchema.toProtobuf(columnSchema))
            .collect(toList());
    builder.setDatabaseName(databaseName)
        .setTableName(tableName)
        .addAllColumns(columnSchemaProtos)
        .setNumReplicas(numReplicas);
    if(isVersionEnabled != null) {
      builder.setIsVersionEnabled(isVersionEnabled);
    }
    if(storageType != null) {
      builder.setStorageType(storageType.toProto());
    }
    if(usedCapacityInBytes > 0 || usedCapacityInBytes == OzoneConsts.USED_CAPACITY_IN_BYTES_RESET) {
      builder.setUsedCapacityInBytes(usedCapacityInBytes);
    }
    return builder.build();
  }

  /**
   * Parses TableInfo protobuf and creates OmTableArgs.
   * @param tableArgs
   * @return instance of OmTableArgs
   */
  public static OmTableArgs getFromProtobuf(TableArgs tableArgs) {
   List<ColumnSchema> columns =  tableArgs.getColumnsList()
           .stream()
           .map(proto -> ColumnSchema.fromProtobuf(proto))
           .collect(toList());
    return new OmTableArgs(tableArgs.getDatabaseName(),
        tableArgs.getTableName(),
            tableArgs.hasIsVersionEnabled() ?
            tableArgs.getIsVersionEnabled() : null,
        tableArgs.hasStorageType() ? StorageType.valueOf(
            tableArgs.getStorageType()) : null,
        columns,
        tableArgs.getNumReplicas(),
        KeyValueUtil.getFromProtobuf(tableArgs.getMetadataList()),
        tableArgs.getUsedCapacityInBytes());
  }
}
