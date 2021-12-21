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
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.client.OzoneQuota;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hetu.client.io.HetuInputStream;
import org.apache.hadoop.hetu.client.io.HetuOutputStream;
import org.apache.hadoop.hetu.client.protocol.ClientProtocol;
import org.apache.hadoop.hetu.photon.helpers.PartialRow;
import org.apache.hadoop.hetu.photon.meta.DistributedKeyType;
import org.apache.hadoop.hetu.photon.meta.PartitionKeyType;
import org.apache.hadoop.hetu.photon.meta.schema.ColumnSchema;
import org.apache.hadoop.hetu.photon.meta.schema.Schema;
import org.apache.hadoop.hetu.photon.proto.HetuPhotonProtos;
import org.apache.hadoop.ozone.om.helpers.WithMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import static org.apache.hadoop.ozone.OzoneConsts.QUOTA_RESET;

/**
 * A class that encapsulates OzoneDatabase.
 */
public class OzoneDatabase extends WithMetadata {
  static final Logger LOG = LoggerFactory.getLogger(OzoneDatabase.class);

  /**
   * The proxy used for connecting to the cluster and perform
   * client operations.
   */
  private final ClientProtocol proxy;

  /**
   * Name of the Database.
   */
  private final String databaseName;

  /**
   * Admin Name of the Database.
   */
  private String admin;
  /**
   * Owner of the Database.
   */
  private String owner;
  /**
   * Quota of bytes allocated for the Database.
   */
  private long quotaInBytes;
  /**
   * Quota of table count allocated for the Database.
   */
  private long quotaInTable;
  /**
   * Table namespace quota usage.
   */
  private long usedNamespace;
  /**
   * Creation time of the Database.
   */
  private Instant creationTime;
  /**
   * Modification time of the database.
   */
  private Instant modificationTime;

  private int listCacheSize;

  /**
   * Constructs OzoneDatabase instance.
   * @param conf Configuration object.
   * @param proxy ClientProtocol proxy.
   * @param databaseName Name of the database.
   * @param admin Database admin.
   * @param owner Database owner.
   * @param quotaInBytes Database quota in bytes.
   * @param creationTime creation time of the database
   * @param metadata custom key value metadata.
   */
  @SuppressWarnings("parameternumber")
  public OzoneDatabase(ConfigurationSource conf, ClientProtocol proxy,
                       String databaseName, String admin, String owner, long quotaInBytes,
                       int quotaInTable, long creationTime,
                       Map<String, String> metadata) {
    Preconditions.checkNotNull(proxy, "Client proxy is not set.");
    this.proxy = proxy;
    this.databaseName = databaseName;
    this.admin = admin;
    this.owner = owner;
    this.quotaInBytes = quotaInBytes;
    this.quotaInTable = quotaInTable;
    this.creationTime = Instant.ofEpochMilli(creationTime);
    this.listCacheSize = HddsClientUtils.getListCacheSize(conf);
    this.metadata = metadata;
    modificationTime = Instant.now();
    if (modificationTime.isBefore(this.creationTime)) {
      modificationTime = Instant.ofEpochSecond(
          this.creationTime.getEpochSecond(), this.creationTime.getNano());
    }
  }

  /**
   * @param modificationTime modification time of the database.
   */
  @SuppressWarnings("parameternumber")
  public OzoneDatabase(ConfigurationSource conf, ClientProtocol proxy,
                       String databaseName, String admin, String owner, long quotaInBytes,
                       int quotaInTable, long usedNamespace, long creationTime,
                       long modificationTime,
                       Map<String, String> metadata) {
    this(conf, proxy, databaseName, admin, owner, quotaInBytes, quotaInTable,
        creationTime, metadata);
    this.modificationTime = Instant.ofEpochMilli(modificationTime);
    this.usedNamespace = usedNamespace;
  }

  @SuppressWarnings("parameternumber")
  public OzoneDatabase(ConfigurationSource conf, ClientProtocol proxy,
                       String databaseName, String admin, String owner, long quotaInBytes,
                       int quotaInTable, long creationTime) {
    this(conf, proxy, databaseName, admin, owner, quotaInBytes, quotaInTable,
        creationTime, new HashMap<>());
    modificationTime = Instant.now();
    if (modificationTime.isBefore(this.creationTime)) {
      modificationTime = Instant.ofEpochSecond(
          this.creationTime.getEpochSecond(), this.creationTime.getNano());
    }
  }

  @SuppressWarnings("parameternumber")
  public OzoneDatabase(ConfigurationSource conf, ClientProtocol proxy,
                       String databaseName, String admin, String owner, long quotaInBytes,
                       int quotaInTable, long usedNamespace, long creationTime,
                       long modificationTime) {
    this(conf, proxy, databaseName, admin, owner, quotaInBytes, quotaInTable,
        creationTime);
    this.modificationTime = Instant.ofEpochMilli(modificationTime);
    this.usedNamespace = usedNamespace;
  }

  @VisibleForTesting
  protected OzoneDatabase(String databaseName, String admin, String owner,
                          long quotaInBytes, int quotaInTable, long creationTime) {
    this.proxy = null;
    this.databaseName = databaseName;
    this.admin = admin;
    this.owner = owner;
    this.quotaInBytes = quotaInBytes;
    this.quotaInTable = quotaInTable;
    this.creationTime = Instant.ofEpochMilli(creationTime);
    this.metadata = new HashMap<>();
    modificationTime = Instant.now();
    if (modificationTime.isBefore(this.creationTime)) {
      modificationTime = Instant.ofEpochSecond(
          this.creationTime.getEpochSecond(), this.creationTime.getNano());
    }
  }

  @SuppressWarnings("parameternumber")
  @VisibleForTesting
  protected OzoneDatabase(String databaseName, String admin, String owner,
                          long quotaInBytes, int quotaInTable, long creationTime,
                          long modificationTime) {
    this(databaseName, admin, owner, quotaInBytes, quotaInTable, creationTime);
    this.modificationTime = Instant.ofEpochMilli(modificationTime);
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
   * Returns Database's admin name.
   *
   * @return adminName
   */
  public String getAdmin() {
    return admin;
  }

  /**
   * Returns Database's owner name.
   *
   * @return ownerName
   */
  public String getOwner() {
    return owner;
  }

  /**
   * Returns Quota allocated for the Database in bytes.
   *
   * @return quotaInBytes
   */
  public long getQuotaInBytes() {
    return quotaInBytes;
  }

  /**
   * Returns quota of table counts allocated for the Database.
   *
   * @return quotaInTable
   */
  public long getQuotaInTable() {
    return quotaInTable;
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

  /**
   * Returns used database namespace.
   * @return usedNamespace
   */
  public long getUsedNamespace() {
    return usedNamespace;
  }

  /**
   * Sets/Changes the owner of this Database.
   * @param userName new owner
   * @throws IOException
   */
  public boolean setOwner(String userName) throws IOException {
    boolean result = proxy.setDatabaseOwner(databaseName, userName);
    this.owner = userName;
    return result;
  }

  /**
   * Clean the space quota of the database.
   *
   * @throws IOException
   */
  public void clearSpaceQuota() throws IOException {
    OzoneDatabase ozoneDatabase = proxy.getDatabaseDetails(databaseName);
    proxy.setDatabaseQuota(databaseName, ozoneDatabase.getQuotaInTable(), QUOTA_RESET);
    this.quotaInBytes = QUOTA_RESET;
    this.quotaInTable = ozoneDatabase.getQuotaInTable();
  }

  /**
   * Clean the namespace quota of the database.
   *
   * @throws IOException
   */
  public void clearNamespaceQuota() throws IOException {
    OzoneDatabase ozoneDatabase = proxy.getDatabaseDetails(databaseName);
    proxy.setDatabaseQuota(databaseName, QUOTA_RESET, ozoneDatabase.getQuotaInBytes());
    this.quotaInBytes = ozoneDatabase.getQuotaInBytes();
    this.quotaInTable = QUOTA_RESET;
  }

  /**
   * Sets/Changes the quota of this Database.
   *
   * @param quota OzoneQuota Object that can be applied to storage database.
   * @throws IOException
   */
  public void setQuota(OzoneQuota quota) throws IOException {
    proxy.setDatabaseQuota(databaseName, quota.getQuotaInNamespace(),
        quota.getQuotaInBytes());
    this.quotaInBytes = quota.getQuotaInBytes();
    this.quotaInTable = quota.getQuotaInNamespace();
  }

  /**
   * Creates a new Table in this Database, with default values.
   * @param tableName Name of the Table
   * @throws IOException
   */
  public void createTable(String tableName)
      throws IOException {
    proxy.createTable(databaseName, tableName);
  }

  /**
   * Creates a new Table in this Database, with properties set in tableArgs.
   * @param tableName Name of the Table
   * @param tableArgs Properties to be set
   * @throws IOException
   */
  public void createTable(String tableName, TableArgs tableArgs)
      throws IOException {
    Schema schema = tableArgs.getSchema();
    proxy.createTable(databaseName, tableName, tableArgs);
    if (tableArgs.getSchema().getPartitionKey().getPartitionKeyType().equals(PartitionKeyType.HASH)) {
      // TODO: init hash partition
      for (int i=0; i < schema.getPartitionKey().getPartitions(); i++) {
        PartitionArgs partitionArgs = PartitionArgs.newBuilder()
                .setDatabaseName(tableArgs.getDatabaseName())
                .setTableName(tableArgs.getTableName())
                .setPartitionName("part" + i)
                .setPartitionValue("hash partition")
                .build();
        OzoneTable ozoneTable = this.getTable(tableName);
        ozoneTable.createPartition(partitionArgs.getPartitionName(), partitionArgs);
      }
    }
  }

  /**
   * Get the Table from this Database.
   * @param tableName Name of the Table
   * @return OzoneTable
   * @throws IOException
   */
  public OzoneTable getTable(String tableName) throws IOException {
    OzoneTable table = proxy.getTableDetails(databaseName, tableName);
    return table;
  }

  /**
   * Returns Iterator to iterate over all tables in the database.
   * The result can be restricted using table prefix, will return all
   * tables if table prefix is null.
   *
   * @param tablePrefix Table prefix to match
   * @return {@code Iterator<OzoneTable>}
   */
  public Iterator<? extends OzoneTable> listTables(String tablePrefix) {
    return listTables(tablePrefix, null);
  }

  /**
   * Returns Iterator to iterate over all tables after prevTable in the
   * database.
   * If prevTable is null it iterates from the first table in the database.
   * The result can be restricted using table prefix, will return all
   * tables if table prefix is null.
   *
   * @param tablePrefix Table prefix to match
   * @param prevTable Tables are listed after this table
   * @return {@code Iterator<OzoneTable>}
   */
  public Iterator<? extends OzoneTable> listTables(String tablePrefix,
      String prevTable) {
    return new TableIterator(tablePrefix, prevTable);
  }

  /**
   * Deletes the Table from this Database.
   * @param tableName Name of the Database
   * @throws IOException
   */
  public void deleteTable(String tableName) throws IOException {
    proxy.deleteTable(databaseName, tableName);
  }

  /**
   * Rename the database name from fromDatabaseName to toDatabaseName.
   * @param fromDatabaseName The original database name.
   * @param toDatabaseName New Database name.
   * @throws IOException
   */
  public void renameDatabase(String fromDatabaseName, String toDatabaseName)
          throws IOException {
    proxy.renameDatabase(fromDatabaseName, toDatabaseName);
  }

  /**
   * Rename the table name from fromTableName to toTableName.
   * @param fromTableName The original Table name.
   * @param toTableName New Table name.
   * @throws IOException
   */
  public void renameTable(String fromTableName, String toTableName)
          throws IOException {
    proxy.renameTable(databaseName, fromTableName, toTableName);
  }

  /**
   * Rename the table by tableMap, The table is fromTableName and value is toTableName.
   * @param tableMap The table is original table name nad value is new table name.
   * @throws IOException
   */
  public void renameTables(Map<String, String> tableMap)
          throws IOException {
    proxy.renameTables(databaseName, tableMap);
  }

  /**
   * An Iterator to iterate over {@link OzoneTable} list.
   */
  private class TableIterator implements Iterator<OzoneTable> {

    private String tablePrefix = null;

    private Iterator<OzoneTable> currentIterator;
    private OzoneTable currentValue;


    /**
     * Creates an Iterator to iterate over all tables after prevTable in
     * the database.
     * If prevTable is null it iterates from the first table in the database.
     * The returned tables match table prefix.
     * @param tablePrefix
     */
    TableIterator(String tablePrefix, String prevTable) {
      this.tablePrefix = tablePrefix;
      this.currentValue = null;
      this.currentIterator = getNextListOfTables(prevTable).iterator();
    }

    @Override
    public boolean hasNext() {
      if (!currentIterator.hasNext() && currentValue != null) {
        currentIterator = getNextListOfTables(currentValue.getTableName())
            .iterator();
      }
      return currentIterator.hasNext();
    }

    @Override
    public OzoneTable next() {
      if(hasNext()) {
        currentValue = currentIterator.next();
        return currentValue;
      }
      throw new NoSuchElementException();
    }

    /**
     * Gets the next set of table list using proxy.
     * @param prevTable
     * @return {@code List<OzoneTable>}
     */
    private List<OzoneTable> getNextListOfTables(String prevTable) {
      try {
        return proxy.listTables(databaseName, tablePrefix, prevTable, listCacheSize);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private void insertPartition(String tableName, String partitionName, PartialRow... rows)
          throws IOException, HetuClientException {
    OzoneTable ozoneTable = getTable(tableName);
    OzonePartition ozonePartition = ozoneTable.getPartition(partitionName);
    Schema schema = ozoneTable.getSchema();
    DistributedKeyType distributedKeyType = schema.getDistributedKey().getDistributedKeyType();
    List<String> distributedKeyFields = schema.getDistributedKey().getFields();
    List<ColumnSchema> distributedKeyCols = distributedKeyFields.stream()
            .map(colName -> schema.getColumn(colName))
            .collect(Collectors.toList());

    int buckets = schema.getDistributedKey().getBuckets();
    if (distributedKeyType.equals(DistributedKeyType.HASH)) {
      Arrays.stream(rows).forEach(row -> {
        // TODO: primary key or unique key
        byte[] colKey = row.encodeColumnKey();
        HashCode hashCode = Hashing.sha256().hashBytes(colKey);
        int bucketNum = Hashing.consistentHash(hashCode, buckets);
        String tabletName = "tablet_" + bucketNum;
        HetuOutputStream hetuOutputStream = null;
        byte[] data = row.toProtobuf().toByteArray();
        try {
          LOG.info("Insert into [{}.{}], row: {}", partitionName, tabletName, row);
          hetuOutputStream = ozonePartition.openTablet(databaseName, tableName,
                  partitionName, tabletName, data.length,
                  new RatisReplicationConfig(HddsProtos.ReplicationFactor.ONE));
          hetuOutputStream.write(data);
        } catch (IOException e) {
          e.printStackTrace();
        } finally {
          // TODO close output stream
          try {
              if (hetuOutputStream != null) {
                hetuOutputStream.close();
              }
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      });
    } else if (distributedKeyType.equals(DistributedKeyType.LIST)) {
      throw new UnsupportedOperationException("Unsupported distributedKeyType = "
              + distributedKeyType);
    } else if (distributedKeyType.equals(DistributedKeyType.RANGE)) {
      throw new UnsupportedOperationException("Unsupported distributedKeyType = "
              + distributedKeyType);
    } else {
      throw new HetuClientException(
              "BUG: DistributedKeyType not found, type = "
                      + distributedKeyType);
    }
  }

  public void insertTable(String tableName, PartialRow... rows)
          throws IOException, HetuClientException {
    OzoneTable ozoneTable = getTable(tableName);
    Schema schema = ozoneTable.getSchema();
    PartitionKeyType partitionKeyType = schema.getPartitionKey().getPartitionKeyType();
    List<String> partitionKeyFields = schema.getPartitionKey().getFields();
    List<ColumnSchema> partitionKeyCols = partitionKeyFields.stream()
            .map(colName -> schema.getColumn(colName))
            .collect(Collectors.toList());

    if (partitionKeyType.equals(PartitionKeyType.HASH)) {
      int partitions = schema.getPartitionKey().getPartitions();
      Arrays.stream(rows).forEach(row -> {
        if (partitionKeyCols.size() == 1) {
          ColumnSchema columnSchema = partitionKeyCols.get(0);
          HashCode hashCode = getPartitionFieldsValueByHashCode(row, columnSchema);
          int partitionNum = Hashing.consistentHash(hashCode, partitions);
          try {
            String partitionName = "part" + partitionNum;
            insertPartition(tableName, partitionName, row);
          } catch (IOException e) {
            e.printStackTrace();
          } catch (HetuClientException e) {
            e.printStackTrace();
          }
        } else {
          throw new RuntimeException("Partition key type: " + partitionKeyType
                  + " of fields size != 1 : " + partitionKeyFields);
        }
      });
    } else if (partitionKeyType.equals(PartitionKeyType.LIST)) {
      throw new UnsupportedOperationException("Unsupported partitionKeyType = "
              + partitionKeyType);
    } else if (partitionKeyType.equals(PartitionKeyType.RANGE)) {
      throw new UnsupportedOperationException("Unsupported partitionKeyType = "
              + partitionKeyType);
    } else {
      throw new HetuClientException(
              "BUG: PartitionKeyType not found, type = "
                      + partitionKeyType);
    }
  }

  private HashCode getPartitionFieldsValueByHashCode(PartialRow row, ColumnSchema columnSchema) {
    HashCode hashCode;
    switch (columnSchema.getColumnType()) {
      case BOOL:
      case INT8:
      case INT16:
        hashCode = Hashing.sha256().hashInt(row.getShort(columnSchema.getColumnName()));
        break;
      case INT32:
        hashCode = Hashing.sha256().hashInt(row.getInt(columnSchema.getColumnName()));
        break;
      case INT64:
        hashCode = Hashing.sha256().hashLong(row.getInt(columnSchema.getColumnName()));
        break;
      case DOUBLE:
      case FLOAT:
      case STRING:
        hashCode = Hashing.sha256().hashBytes(row.getString(columnSchema.getColumnName()).getBytes());
        break;
      case DATE:
        hashCode = Hashing.sha256().hashLong(row.getDate(columnSchema.getColumnName()).getTime());
        break;
      case DECIMAL:
      case BINARY:
      case VARCHAR:
        hashCode = Hashing.sha256().hashBytes(row.getVarchar(columnSchema.getColumnName()).getBytes());
        break;
      case UNIXTIME_MICROS:
        hashCode = Hashing.sha256().hashLong(row.getTimestamp(columnSchema.getColumnName()).getTime());
        break;
      default:
        throw new RuntimeException("Unsupported data type: " + columnSchema.getColumnType()
                + " of column name: " + columnSchema.getColumnName());
    }
    return hashCode;
  }

  public List<PartialRow> scanQueryTable(String tableName, String queryBuilder)
          throws IOException, HetuClientException {
    OzoneTable ozoneTable = getTable(tableName);
    Schema schema = ozoneTable.getSchema();
    PartitionKeyType partitionKeyType = schema.getPartitionKey().getPartitionKeyType();
    List<String> partitionKeyFields = schema.getPartitionKey().getFields();
    List<ColumnSchema> partitionKeyCols = partitionKeyFields.stream()
            .map(colName -> schema.getColumn(colName))
            .collect(Collectors.toList());

    if (partitionKeyType.equals(PartitionKeyType.HASH)) {
      Iterator<OzonePartition> it = (Iterator<OzonePartition>) ozoneTable.listPartitions("part");
      List<PartialRow> rows = new ArrayList<>();
      while (it.hasNext()) {
        OzonePartition ozonePartition = it.next();
        List<PartialRow> scanInRows = scanQueryPartition(ozonePartition.getTableName(), ozonePartition.getPartitionName(), queryBuilder);
        rows.addAll(scanInRows);
      }
      return rows;
    } else if (partitionKeyType.equals(DistributedKeyType.LIST)) {
      throw new UnsupportedOperationException("Unsupported partitionKeyType = "
              + partitionKeyType);
    } else if (partitionKeyType.equals(DistributedKeyType.RANGE)) {
      throw new UnsupportedOperationException("Unsupported partitionKeyType = "
              + partitionKeyType);
    } else {
      throw new HetuClientException(
              "BUG: PartitionKeyType not found, type = "
                      + partitionKeyType);
    }
  }

  private List<PartialRow> scanQueryPartition(String tableName, String partitionName, String queryBuilder)
          throws IOException, HetuClientException {
    OzoneTable ozoneTable = getTable(tableName);
    OzonePartition ozonePartition = ozoneTable.getPartition(partitionName);
    Schema schema = ozoneTable.getSchema();
    DistributedKeyType distributedKeyType = schema.getDistributedKey().getDistributedKeyType();
    List<String> distributedKeyFields = schema.getDistributedKey().getFields();
    List<ColumnSchema> distributedKeyCols = distributedKeyFields.stream()
            .map(colName -> schema.getColumn(colName))
            .collect(Collectors.toList());

    if (distributedKeyType.equals(DistributedKeyType.HASH)) {
      int buckets = schema.getDistributedKey().getBuckets();
      List<PartialRow> rows = new ArrayList<>();
      Iterator<OzoneTablet> it = (Iterator<OzoneTablet>) ozonePartition.listTablets("tablet_");
      HetuInputStream hetuInputStream;
      while (it.hasNext()) {
        OzoneTablet ozoneTablet = it.next();
        hetuInputStream = ozonePartition.readTablet(ozoneTablet.getTabletName());
        byte[] content = IOUtils.toByteArray(hetuInputStream);
        LOG.info("Scan query [{}.{}], length: {}", partitionName,
                ozoneTablet.getTabletName(), content.length);
        List<PartialRow> tabletRows = PartialRow.deserialize(content);
        rows.addAll(tabletRows);
        LOG.info("Scan query [{}.{}], tablet rows: {}", partitionName,
                ozoneTablet.getTabletName(), tabletRows.size());
      }
      return rows;
    } else if (distributedKeyType.equals(DistributedKeyType.LIST)) {
      throw new UnsupportedOperationException("Unsupported distributedKeyType = "
              + distributedKeyType);
    } else if (distributedKeyType.equals(DistributedKeyType.RANGE)) {
      throw new UnsupportedOperationException("Unsupported distributedKeyType = "
              + distributedKeyType);
    } else {
      throw new HetuClientException(
              "BUG: DistributedKeyType not found, type = "
                      + distributedKeyType);
    }
  }

  public void addPartition(PartitionArgs partitionArgs) throws IOException, HetuClientException {
    OzoneTable ozoneTable = getTable(partitionArgs.getTableName());
    int buckets = ozoneTable.getBuckets();
    Schema schema = ozoneTable.getSchema();
    PartitionKeyType partitionKeyType = schema.getPartitionKey().getPartitionKeyType();
    List<String> fields = schema.getPartitionKey().getFields();

    OzonePartition ozonePartition = ozoneTable.getPartition(partitionArgs.getPartitionName());
    if (partitionKeyType.equals(PartitionKeyType.HASH)) {
      throw new HetuClientException(
              "BUG: Add partition, not support partitionKeyType = "
                      + partitionKeyType);
    } else if (partitionKeyType.equals(DistributedKeyType.LIST)) {
      throw new UnsupportedOperationException("Unsupported partitionKeyType = "
              + partitionKeyType);
    } else if (partitionKeyType.equals(DistributedKeyType.RANGE)) {
      throw new UnsupportedOperationException("Unsupported partitionKeyType = "
              + partitionKeyType);
    } else {
      throw new HetuClientException(
              "BUG: PartitionKeyType not found, type = "
                      + partitionKeyType);
    }
  }

}