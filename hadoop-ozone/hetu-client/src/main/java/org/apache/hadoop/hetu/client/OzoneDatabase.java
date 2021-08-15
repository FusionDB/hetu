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
import org.apache.hadoop.hdds.client.OzoneQuota;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hetu.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.helpers.WithMetadata;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.apache.hadoop.ozone.OzoneConsts.QUOTA_RESET;

/**
 * A class that encapsulates OzoneDatabase.
 */
public class OzoneDatabase extends WithMetadata {

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
    proxy.createTable(databaseName, tableName, tableArgs);
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
}