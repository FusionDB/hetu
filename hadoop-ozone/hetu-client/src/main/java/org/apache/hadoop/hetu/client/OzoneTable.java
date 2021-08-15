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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hetu.client.protocol.ClientProtocol;
import org.apache.hadoop.hetu.hm.meta.table.ColumnKey;
import org.apache.hadoop.hetu.hm.meta.table.ColumnSchema;
import org.apache.hadoop.hetu.hm.meta.table.Schema;
import org.apache.hadoop.hetu.hm.meta.table.StorageEngine;
import org.apache.hadoop.ozone.audit.SCMAction;
import org.apache.hadoop.ozone.om.helpers.WithMetadata;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
        .TableInfo.StorageEngineProto;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * A class that encapsulates OzoneTable.
 */
public class OzoneTable extends WithMetadata {

  /**
   * The proxy used for connecting to the cluster and perform
   * client operations.
   */
  private final ClientProtocol proxy;

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
   * Table storage engine: LStore or CStore
   */
  private StorageEngine storageEngine;

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
  private Instant creationTime;
  /**
   * modification time of table.
   */
  private Instant modificationTime;
  /**
   * Table num replica: 1 or 3 or 2n+1
   */
  private int numReplicas;
  /**
   * Table num buckets: 8 ~ 64
   */
  private int buckets;
  /**
   * Table of usedBytes
   */
  private long usedBytes;

  /**
   * Table of quotaInBytes
   */
  private long quotaInBytes;

  /**
   * Table of usedBucket
   */
  private int usedBucket;

  /**
   * Table of quotaInBucket
   */
  private int quotaInBucket;

  private int listCacheSize;

  /**
   * Constructs OzoneTable instance.
   * @param conf Configuration object.
   * @param proxy ClientProtocol proxy.
   * @param databaseName Name of the database.
   * @param tableName Name of the table.
   * @param schema Schema of the table.
   * @param usedBytes Table used in bytes.
   * @param creationTime creation time of the table
   * @param metadata custom key value metadata.
   */
  @SuppressWarnings("parameternumber")
  public OzoneTable(ConfigurationSource conf, ClientProtocol proxy,
                    String databaseName, String tableName,
                    Schema schema, StorageEngine storageEngine, StorageType storageType,
                    int numReplicas, int buckets, long usedBytes, long quotaInBytes,
                    int usedBucket, int quotaInBucket, long creationTime,
                    boolean isVersionEnabled, Map<String, String> metadata) {
    Preconditions.checkNotNull(proxy, "Client proxy is not set.");
    this.proxy = proxy;
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.schema = schema;
    this.storageEngine = storageEngine;
    this.storageType = storageType;
    this.numReplicas = numReplicas;
    this.buckets = buckets;
    this.usedBytes = usedBytes;
    this.quotaInBytes = quotaInBytes;
    this.usedBucket = usedBucket;
    this.quotaInBucket = quotaInBucket;
    this.creationTime = Instant.ofEpochMilli(creationTime);
    this.listCacheSize = HddsClientUtils.getListCacheSize(conf);
    this.metadata = metadata;
    this.isVersionEnabled = isVersionEnabled;
    modificationTime = Instant.now();
    if (modificationTime.isBefore(this.creationTime)) {
      modificationTime = Instant.ofEpochSecond(
              this.creationTime.getEpochSecond(), this.creationTime.getNano());
    }
  }

  /**
   * @param modificationTime modification time of the table.
   */
  @SuppressWarnings("parameternumber")
  public OzoneTable(ConfigurationSource conf, ClientProtocol proxy,
                    String databaseName, String tableName,
                    Schema schema, StorageEngine storageEngine, StorageType storageType,
                    int numReplicas, int buckets, long usedBytes, long quotaInBytes,
                    int usedBucket, int quotaInBucket, long creationTime,
                    long modificationTime, Map<String, String> metadata) {
    this(conf, proxy, databaseName, tableName, schema, storageEngine,
            storageType, numReplicas, buckets, usedBytes, quotaInBytes,
            usedBucket, quotaInBucket, creationTime, false, metadata);
    this.modificationTime = Instant.ofEpochMilli(modificationTime);
  }

  @SuppressWarnings("parameternumber")
  public OzoneTable(ConfigurationSource conf, ClientProtocol proxy,
                    String databaseName, String tableName,
                    Schema schema, long usedBytes, int usedBucket, long creationTime,
                    boolean isVersionEnabled, Map<String, String> metadata) {
    this.proxy = proxy;
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.schema = schema;
    this.usedBytes = usedBytes;
    this.usedBucket = usedBucket;
    this.creationTime = Instant.ofEpochMilli(creationTime);
    this.metadata = metadata;
    this.listCacheSize = HddsClientUtils.getListCacheSize(conf);

    this.numReplicas = 3;
    this.storageType = StorageType.DEFAULT;
    this.storageEngine = StorageEngine.LSTORE;
    this.isVersionEnabled = isVersionEnabled;
    modificationTime = Instant.now();
    if (modificationTime.isBefore(this.creationTime)) {
      modificationTime = Instant.ofEpochSecond(
              this.creationTime.getEpochSecond(), this.creationTime.getNano());
    }
  }

  @SuppressWarnings("parameternumber")
  public OzoneTable(ConfigurationSource conf, ClientProtocol proxy,
                    String databaseName, String tableName, Schema schema,
                    long usedBytes, int usedBucket, long creationTime) {
    this(conf, proxy, databaseName, tableName, schema,
            usedBytes, usedBucket, creationTime, false, new HashMap<>());
  }


  @SuppressWarnings("parameternumber")
  public OzoneTable(ConfigurationSource conf, ClientProtocol proxy,
                    String databaseName, String tableName,
                    Schema schema, long usedBytes, int usedBucket,
                    long creationTime, Map<String, String> metadata) {
    this(conf, proxy, databaseName, tableName, schema, usedBytes, usedBucket, creationTime, false, metadata);
  }

  @VisibleForTesting
  protected OzoneTable(String databaseName, String tableName, Schema schema,
                       long usedBytes, int usedBucket, long creationTime) {
    this(null, null, databaseName, tableName, schema, usedBytes, usedBucket,
            creationTime, false, new HashMap<>());
  }

  @SuppressWarnings("parameternumber")
  @VisibleForTesting
  protected OzoneTable(String databaseName, String tableName,
                       Schema schema, long usedBytes, long quotaInBytes, long creationTime,
                       long modificationTime) {
    this.proxy = null;
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.schema = schema;
    this.usedBytes = usedBytes;
    this.quotaInBytes = quotaInBytes;
    this.creationTime = Instant.ofEpochMilli(creationTime);
    this.metadata = new HashMap<>();
    this.modificationTime = Instant.ofEpochMilli(modificationTime);
    if (this.modificationTime.isBefore(this.creationTime)) {
      this.modificationTime = Instant.ofEpochSecond(
              this.creationTime.getEpochSecond(), this.creationTime.getNano());
    }
  }

  /**
   * Returns Database name.
   *
   * @return databaseName
   */
  public String getDatabaseName() {
    return databaseName;
  }

  /**
   * Returns Table name.
   *
   * @return tableName
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * Returns Used allocated for the Table in bytes.
   *
   * @return usedInBytes
   */
  public long getUsedBytes() {
    return usedBytes;
  }

  /**
   * Returns quota of buckets counts allocated for the Table.
   *
   * @return usedInBucket
   */
  public long getUsedBucket() {
    return usedBucket;
  }

  /**
   * Returns quota of bytes for the Table.
   * Return
   * @return
   */
  public long getQuotaInBytes() {
    return quotaInBytes;
  }

  /**
   * Returns quota of buckets count for the Table.
   * @return
   */
  public int getQuotaInBucket() {
    return quotaInBucket;
  }

  /**
   * Returns creation time of the database.
   *
   * @return creation time.
   */
  public Instant getCreationTime() {
    return creationTime;
  }

  /**
   * Returns modification time of the database.
   *
   * @return modification time.
   */
  public Instant getModificationTime() {
    return modificationTime;
  }

  public Schema getSchema() {
    return schema;
  }

  public StorageEngine getStorageEngine() {
    return storageEngine;
  }

  public StorageType getStorageType() {
    return storageType;
  }

  public int getNumReplicas() {
    return numReplicas;
  }

  public int getBuckets() {
    return buckets;
  }

  /**
   * Sets/Changes the quota* of this Table.
   * @param quotaInBucket quota in bucket counts of the table
   * @param quotaInBytes  quota in bytes of the table
   * @throws IOException
   */
  public void setTableQuota(int quotaInBucket, long quotaInBytes) throws IOException {
    proxy.setTableQuota(databaseName, tableName, quotaInBucket, quotaInBytes);
  }

  /**
   * Creates a new partition in this Table, with default values.
   * @param partitionName Name of the Partition
   * @throws IOException
   */
  public void createPartition(String partitionName)
          throws IOException {
    proxy.createPartition(databaseName, tableName, partitionName);
  }

  /**
   * Creates a new Partition in this Table, with properties set in partitionArgs.
   * @param partitionName Name of the Partition
   * @param partitionArgs Properties to be set
   * @throws IOException
   */
  public void createPartition(String partitionName, PartitionArgs partitionArgs)
          throws IOException {
    proxy.createPartition(databaseName, tableName, partitionName, partitionArgs);
  }

  /**
   * Get the Partition from this Table.
   * @param partitionName Name of the Partition
   * @return OzonePartition
   * @throws IOException
   */
  public OzonePartition getPartition(String partitionName) throws IOException {
    OzonePartition partition = proxy.getPartitionDetails(databaseName, tableName, partitionName);
    return partition;
  }

  /**
   * Returns Iterator to iterate over all partitions in the table.
   * The result can be restricted using partition prefix, will return all
   * partitions if partition prefix is null.
   *
   * @param partitionPrefix Partition prefix to match
   * @return {@code Iterator<OzonePartition>}
   */
  public Iterator<? extends OzonePartition> listPartitions(String partitionPrefix) {
    return listPartitions(partitionPrefix, null);
  }

  /**
   * Returns Iterator to iterate over all partitions after prevPartition in the
   * table.
   * If prevPartition is null it iterates from the first partition in the table.
   * The result can be restricted using partition prefix, will return all
   * partitions if partition prefix is null.
   *
   * @param partitionPrefix Partition prefix to match
   * @param prevPartition Partitions are listed after this partition
   * @return {@code Iterator<OzonePartition>}
   */
  public Iterator<? extends OzonePartition> listPartitions(String partitionPrefix,
                                                   String prevPartition) {
    return new PartitionIterator(partitionPrefix, prevPartition);
  }

  /**
   * Deletes the Partition from this Table
   * @param partitionName Name of the Table
   * @throws IOException
   */
  public void deletePartition(String partitionName) throws IOException {
    proxy.deletePartition(databaseName, tableName, partitionName);
  }


  /**
   * An Iterator to iterate over {@link OzonePartition} list.
   */
  private class PartitionIterator implements Iterator<OzonePartition> {

    private String partitionPrefix = null;

    private Iterator<OzonePartition> currentIterator;
    private OzonePartition currentValue;


    /**
     * Creates an Iterator to iterate over all partitions after prevPartition in
     * the table.
     * If prevPartition is null it iterates from the first partition in the table.
     * The returned partitions match partition prefix.
     * @param partitionPrefix
     */
    PartitionIterator(String partitionPrefix, String prevPartition) {
      this.partitionPrefix = partitionPrefix;
      this.currentValue = null;
      this.currentIterator = getNextListOfPartitions(prevPartition).iterator();
    }

    @Override
    public boolean hasNext() {
      if (!currentIterator.hasNext() && currentValue != null) {
        currentIterator = getNextListOfPartitions(currentValue.getPartitionName())
                .iterator();
      }
      return currentIterator.hasNext();
    }

    @Override
    public OzonePartition next() {
      if(hasNext()) {
        currentValue = currentIterator.next();
        return currentValue;
      }
      throw new NoSuchElementException();
    }

    /**
     * Gets the next set of partition list using proxy.
     * @param prevPartition
     * @return {@code List<OzonePartition>}
     */
    private List<OzonePartition> getNextListOfPartitions(String prevPartition) {
      try {
        return proxy.listPartitions(databaseName, tableName, partitionPrefix, prevPartition, listCacheSize);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}