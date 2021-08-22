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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hetu.client.io.HetuInputStream;
import org.apache.hadoop.hetu.client.io.HetuOutputStream;
import org.apache.hadoop.hetu.client.io.TabletInputStream;
import org.apache.hadoop.hetu.client.io.TabletOutputStream;
import org.apache.hadoop.hetu.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OzoneTabletStatus;
import org.apache.hadoop.ozone.om.helpers.WithMetadata;
import org.apache.hadoop.ozone.security.auth.HetuObj;
import org.apache.hadoop.ozone.security.auth.HetuObjInfo;
import org.apache.hadoop.util.Time;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * A class that encapsulates OzonePartition.
 */
public class OzonePartition extends WithMetadata {

  /**
   * The proxy used for connecting to the cluster and perform
   * client operations.
   */
  private final ClientProtocol proxy;
  /**
   * Name of the volume in which the table belongs to.
   */
  private final String databaseName;
  /**
   * Name of the table.
   */
  private final String tableName;
  /**
   * Name of the partition.
   */
  private final String partitionName;
  /**
   * Default replication factor to be used while creating keys.
   */
  private final ReplicationConfig defaultReplication;

  /**
   * Type of storage to be used for this table.
   * [RAM_DISK, SSD, DISK, ARCHIVE]
   */
  private StorageType storageType;

  /**
   * Table Version flag.
   */
  private Boolean versioning;

  /**
   * Cache size to be used for listKey calls.
   */
  private int listCacheSize;

  /**
   * Used bytes of the table.
   */
  private long usedBytes;

  /**
   * Used namespace of the table.
   */
  private long usedNamespace;

  /**
   * Creation time of the table.
   */
  private Instant creationTime;

  /**
   * Modification time of the table.
   */
  private Instant modificationTime;

  /**
   * Table Encryption tablet name if table encryption is enabled.
   */
  private String encryptionTabletName;

  private HetuObj hetuObj;

  private String sourceDatabase;
  private String sourceTable;
  private String sourcePartition;

  /**
   * Quota of bytes allocated for the table.
   */
  private long quotaInBytes;
  /**
   * Quota of tablet count allocated for the table.
   */
  private long quotaInNamespace;

  private OzonePartition(ConfigurationSource conf, String databaseName,
                         String tableName, String partitionName,
                         ClientProtocol proxy) {
    Preconditions.checkNotNull(proxy, "Client proxy is not set.");
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.partitionName = partitionName;

    this.defaultReplication = ReplicationConfig.getDefault(conf);

    this.proxy = proxy;
    this.hetuObj = HetuObjInfo.Builder.newBuilder()
        .setDatabaseName(databaseName)
        .setTableName(tableName)
        .setPartitionName(partitionName)
        .setResType(HetuObj.ResourceType.PARTITION)
        .setStoreType(HetuObj.StoreType.OZONE).build();
  }

  @SuppressWarnings("parameternumber")
  public OzonePartition(ConfigurationSource conf, ClientProtocol proxy,
                        String databaseName, String tableName, String partitionName,
                        StorageType storageType, Boolean versioning, long creationTime,
                        Map<String, String> metadata, String encryptionTabletName,
                        String sourceDatabase, String sourceTable, String sourcePartition) {
    this(conf, databaseName, tableName, partitionName, proxy);
    this.storageType = storageType;
    this.versioning = versioning;
    this.listCacheSize = HddsClientUtils.getListCacheSize(conf);
    this.creationTime = Instant.ofEpochMilli(creationTime);
    this.metadata = metadata;
    this.encryptionTabletName = encryptionTabletName;
    this.sourceDatabase = sourceDatabase;
    this.sourceTable = sourceTable;
    this.sourcePartition = sourcePartition;
    modificationTime = Instant.now();
    if (modificationTime.isBefore(this.creationTime)) {
      modificationTime = Instant.ofEpochSecond(
          this.creationTime.getEpochSecond(), this.creationTime.getNano());
    }
  }

  @SuppressWarnings("parameternumber")
  public OzonePartition(ConfigurationSource conf, ClientProtocol proxy,
                        String databaseName, String tabletName, String partitionName,
                        StorageType storageType, Boolean versioning, long creationTime,
                        long modificationTime, Map<String, String> metadata, String encryptionTabletName,
                        String sourceDatabase, String sourceTable, String sourcePartition) {
    this(conf, proxy, databaseName, tabletName, partitionName, storageType, versioning,
        creationTime, metadata, encryptionTabletName, sourceDatabase, sourceTable,
        sourcePartition);
    this.modificationTime = Instant.ofEpochMilli(modificationTime);
  }

  @SuppressWarnings("parameternumber")
  public OzonePartition(ConfigurationSource conf, ClientProtocol proxy,
                        String databaseName, String tableName, String partitionName,
                        StorageType storageType, Boolean versioning, long creationTime,
                        long modificationTime, Map<String, String> metadata, String encryptionTabletName,
                        String sourceDatabase, String sourceTable, String sourcePartition, long usedBytes) {
    this(conf, proxy, databaseName, tableName, partitionName, storageType, versioning,
        creationTime, metadata, encryptionTabletName, sourceDatabase, sourceTable,
        sourcePartition);
    this.usedBytes = usedBytes;
    this.usedNamespace = usedNamespace;
    this.modificationTime = Instant.ofEpochMilli(modificationTime);
  }

  /**
   * Constructs OzonePartition instance.
   * @param conf Configuration object.
   * @param proxy ClientProtocol proxy.
   * @param databaseName Name of the database/table the partition belongs to.
   * @param tableName Name of the table.
   * @param partitionName Name of the partitoin.
   * @param storageType StorageType of the partition.
   * @param versioning versioning status of the partition.
   * @param creationTime creation time of the partition.
   */
  @SuppressWarnings("parameternumber")
  public OzonePartition(ConfigurationSource conf, ClientProtocol proxy,
                        String databaseName, String tableName, String partitionName, StorageType storageType,
                        Boolean versioning, long creationTime, Map<String, String> metadata) {
    this(conf, databaseName, tableName, partitionName, proxy);
    this.storageType = storageType;
    this.versioning = versioning;
    this.listCacheSize = HddsClientUtils.getListCacheSize(conf);
    this.creationTime = Instant.ofEpochMilli(creationTime);
    this.metadata = metadata;
    modificationTime = Instant.now();
    if (modificationTime.isBefore(this.creationTime)) {
      modificationTime = Instant.ofEpochSecond(
          this.creationTime.getEpochSecond(), this.creationTime.getNano());
    }
  }

  /**
   * @param modificationTime modification time of the partition.
   */
  @SuppressWarnings("parameternumber")
  public OzonePartition(ConfigurationSource conf, ClientProtocol proxy,
                        String databaseName, String tableName, String partitionName, StorageType storageType,
                        Boolean versioning, long creationTime, long modificationTime,
                        Map<String, String> metadata) {
    this(conf, proxy, databaseName, tableName, partitionName, storageType, versioning,
        creationTime, metadata);
    this.modificationTime = Instant.ofEpochMilli(modificationTime);
  }

  @VisibleForTesting
  @SuppressWarnings("parameternumber")
  OzonePartition(String databaseName, String tableName, String partitionName,
                 ReplicationConfig defaultReplication,
                 StorageType storageType,
                 Boolean versioning, long creationTime) {
    this.proxy = null;
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.partitionName = partitionName;
    this.defaultReplication = defaultReplication;
    this.storageType = storageType;
    this.versioning = versioning;
    this.creationTime = Instant.ofEpochMilli(creationTime);
    this.hetuObj = HetuObjInfo.Builder.newBuilder()
        .setDatabaseName(databaseName)
        .setTableName(tableName)
        .setPartitionName(partitionName)
        .setResType(HetuObj.ResourceType.PARTITION)
        .setStoreType(HetuObj.StoreType.OZONE).build();
    long modifiedTime = Time.now();
    if (modifiedTime < creationTime) {
      this.modificationTime = Instant.ofEpochMilli(creationTime);
    } else {
      this.modificationTime = Instant.ofEpochMilli(modifiedTime);
    }
  }

  /**
   * Returns Database Name.
   *
   * @return databaseName
   */
  public String getDatabaseName() {
    return databaseName;
  }

  /**
   * Returns Table Name.
   *
   * @return tableName
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * Returns Partition Name.
   *
   * @return partitionName
   */
  public String getPartitionName() {
    return partitionName;
  }

  /**
   * Returns ACL's associated with the Bucket.
   *
   * @return acls
   */
  @JsonIgnore
  public List<OzoneAcl> getAcls() throws IOException {
    return proxy.getAuth(hetuObj);
  }

  /**
   * Returns StorageType of the Table.
   *
   * @return storageType
   */
  public StorageType getStorageType() {
    return storageType;
  }

  /**
   * Returns Versioning associated with the Partition.
   *
   * @return versioning
   */
  public Boolean getVersioning() {
    return versioning;
  }

  /**
   * Returns creation time of the Partition.
   *
   * @return creation time of the partition
   */
  public Instant getCreationTime() {
    return creationTime;
  }

  /**
   * Returns modification time of the Partition.
   *
   * @return modification time of the partition
   */
  public Instant getModificationTime() {
    return modificationTime;
  }

  /**
   * Return the partition encryption tablet name.
   * @return the partition encryption tablet name
   */
  public String getEncryptionTabletName() {
    return encryptionTabletName;
  }

  public String getSourceDatabase() {
    return sourceDatabase;
  }

  public String getSourceTable() {
    return sourceTable;
  }

  public String getSourcePartition() {
    return sourcePartition;
  }

  /**
   * Returns Quota allocated for the Table in bytes.
   *
   * @return quotaInBytes
   */
  public long getQuotaInBytes() {
    return quotaInBytes;
  }

  /**
   * Returns quota of tablet counts allocated for the Table.
   *
   * @return quotaInNamespace
   */
  public long getQuotaInNamespace() {
    return quotaInNamespace;
  }

  /**
   * Builder for OmBucketInfo.
  /**
   * Adds ACLs to the Bucket.
   * @param addAcl ACL to be added
   * @return true - if acl is successfully added, false if acl already exists
   * for the bucket.
   * @throws IOException
   */
  public boolean addAuth(OzoneAcl addAcl) throws IOException {
    return proxy.addAuth(hetuObj, addAcl);
  }

  /**
   * Removes ACLs from the bucket.
   * @return true - if acl is successfully removed, false if acl to be
   * removed does not exist for the bucket.
   * @throws IOException
   */
  public boolean removeAuth(OzoneAcl removeAcl) throws IOException {
    return proxy.removeAuth(hetuObj, removeAcl);
  }

  /**
   * Acls to be set for given Ozone object. This operations reset ACL for
   * given object to list of ACLs provided in argument.
   * @param acls List of acls.
   *
   * @throws IOException if there is error.
   * */
  public boolean setAuth(List<OzoneAcl> acls) throws IOException {
    return proxy.setAuth(hetuObj, acls);
  }

  /**
   * Enable/Disable versioning of the partition.
   * @param newVersioning
   * @throws IOException
   */
  public void setVersioning(Boolean newVersioning) throws IOException {
    // TODO: replace setPartitionProperties
    versioning = newVersioning;
  }

  /**
   * Creates a new tablet in the partition, with default replication type RATIS and
   * with replication factor THREE.
   * @param tablet Name of the tablet to be created.
   * @param size Size of the data the tablet will point to.
   * @return TabletOutputStream to which the data has to be written.
   * @throws IOException
   */
  public HetuOutputStream createTablet(String tablet, long size)
      throws IOException {
    return createTablet(tablet, size, defaultReplication,
        new HashMap<>());
  }

  /**
   * Creates a new tablet in the partitoin.
   * @param tablet Name of the tablet to be created.
   * @param size Size of the data the tablet will point to.
   * @param type Replication type to be used.
   * @param factor Replication factor of the tablet.
   * @return OzoneOutputStream to which the data has to be written.
   * @throws IOException
   */
  @Deprecated
  public HetuOutputStream createTablet(String tablet, long size,
                                         ReplicationType type,
                                         ReplicationFactor factor,
                                         Map<String, String> keyMetadata)
      throws IOException {
    return proxy
        .createTablet(databaseName, tableName, partitionName, tablet, size, type, factor, keyMetadata);
  }

  public HetuOutputStream openTablet(String tablet) {
    return proxy.openTablet(databaseName, tableName, partitionName, tablet);
  }

  /**
   * Creates a new tablet in the partition.
   *
   * @param tablet               Name of the tablet to be created.
   * @param size              Size of the data the tablet will point to.
   * @param replicationConfig Replication configuration.
   * @return OzoneOutputStream to which the data has to be written.
   * @throws IOException
   */
  public HetuOutputStream createTablet(String tablet, long size,
                                       ReplicationConfig replicationConfig,
                                       Map<String, String> keyMetadata)
      throws IOException {
    return proxy
        .createTablet(databaseName, tableName, partitionName, tablet, size, replicationConfig, keyMetadata);
  }

  /**
   * Reads an existing tablet from the partition.
   *
   * @param tablet Name of the tablet to be read.
   * @return OzoneInputStream the stream using which the data can be read.
   * @throws IOException
   */
  public HetuInputStream readTablet(String tablet) throws IOException {
    return proxy.getTablet(databaseName, tableName, partitionName, tablet);
  }

  /**
   * Returns information about the tablet.
   * @param tablet Name of the tablet.
   * @return OzoneTabletDetails Information about the tablet.
   * @throws IOException
   */
  public OzoneTabletDetails getTablet(String tablet) throws IOException {
    return proxy.getTabletDetails(databaseName, tableName, partitionName, tablet);
  }

  public long getUsedBytes() {
    return usedBytes;
  }

  /**
   * Returns Iterator to iterate over all tablets in the partition.
   * The result can be restricted using tablet prefix, will return all
   * tablets if tablet prefix is null.
   *
   * @param tabletPrefix Tablet prefix to match
   * @return {@code Iterator<OzoneTablet>}
   */
  public Iterator<? extends OzoneTablet> listTablets(String tabletPrefix)
      throws IOException{
    return listTablets(tabletPrefix, null);
  }

  /**
   * Returns Iterator to iterate over all tablets after prevTablet in the partition.
   * If prevTablet is null it iterates from the first tablet in the partition.
   * The result can be restricted using tablet prefix, will return all
   * tablets if tablet prefix is null.
   *
   * @param tabletPrefix Tablet prefix to match
   * @param prevTablet Tablets will be listed after this tablet name
   * @return {@code Iterator<OzoneTablet>}
   */
  public Iterator<? extends OzoneTablet> listTablets(String tabletPrefix,
      String prevTablet) throws IOException {
    return new TabletIterator(tabletPrefix, prevTablet);
  }

  /**
   * Deletes tablet from the partition.
   * @param tablet Name of the tablet to be deleted.
   * @throws IOException
   */
  public void deleteTablet(String tablet) throws IOException {
    proxy.deleteTablet(databaseName, tableName, partitionName, tablet);
  }

  /**
   * Deletes the given list of tablets from the partition.
   * @param tabletList List of the tablet name to be deleted.
   * @throws IOException
   */
  public void deleteTablets(List<String> tabletList) throws IOException {
    proxy.deleteTablets(databaseName, tableName, partitionName, tabletList);
  }

  /**
   * Hetu api to get tablet status for an entry.
   *
   * @param tabletName Tablet name
   * @throws OMException if tablet does not exist
   *                     if partition does not exist
   * @throws IOException if there is error in the db
   *                     invalid arguments
   */
  public OzoneTabletStatus getTabletStatus(String tabletName) throws IOException {
    return proxy.getOzoneTabletStatus(databaseName, tableName, partitionName, tabletName);
  }

  /**
   * Hetu api to creates an output stream for a tablet.
   *
   * @param tabletName   Tablet name
   * @param overWrite if true existing tablet at the location will be overwritten
   * @param recursive if true tablet would be created even if parent directories
   *                    do not exist
   * @throws OMException if given tablet is a directory
   *                     if tablet exists and isOverwrite flag is false
   *                     if an ancestor exists as a tablet
   *                     if partition does not exist
   * @throws IOException if there is error in the db
   *                     invalid arguments
   */
  public HetuOutputStream createTablet(String tabletName, long size,
      ReplicationType type, ReplicationFactor factor, boolean overWrite,
      boolean recursive) throws IOException {
    return proxy
        .createTablet(databaseName, tableName, partitionName, tabletName, size, type, factor, overWrite,
            recursive);
  }

  /**
   * Hetu api to creates an output stream for a tablet.
   *
   * @param tabletName   Tablet name
   * @param overWrite if true existing tablet at the location will be overwritten
   * @param recursive if true tablet would be created even if parent directories
   *                    do not exist
   * @throws OMException if given tablet is a directory
   *                     if tablet exists and isOverwrite flag is false
   *                     if an ancestor exists as a tablet
   *                     if partition does not exist
   * @throws IOException if there is error in the db
   *                     invalid arguments
   */
  public HetuOutputStream createTablet(String tabletName, long size,
      ReplicationConfig replicationConfig, boolean overWrite,
      boolean recursive) throws IOException {
    return proxy
        .createTablet(databaseName, tableName, partitionName, tabletName, size, replicationConfig,
            overWrite, recursive);
  }

  /**
   * List the status for a tablet or a directory and its contents.
   *
   * @param tabletName    Absolute path of the entry to be listed
   * @param recursive  For a directory if true all the descendants of a
   *                   particular directory are listed
   * @param startTablet   Tablet from which listing needs to start. If startTablet exists
   *                   its status is included in the final list.
   * @param numEntries Number of entries to list from the start tablet
   * @return list of tablet status
   */
  public List<OzoneTabletStatus> listStatus(String tabletName, boolean recursive,
      String startTablet, long numEntries) throws IOException {
    return proxy
        .listStatus(databaseName, tableName, partitionName, tabletName, recursive, startTablet, numEntries);
  }

  /**
   * An Iterator to iterate over {@link OzoneTablet} list.
   */
  private class TabletIterator implements Iterator<OzoneTablet> {

    private String tabletPrefix = null;
    private Iterator<OzoneTablet> currentIterator;
    private OzoneTablet currentValue;


    /**
     * Creates an Iterator to iterate over all tablets after prevTablet in the partition.
     * If prevTablet is null it iterates from the first tablet in the partition.
     * The returned tablets match tablet prefix.
     * @param tabletPrefix
     */
    TabletIterator(String tabletPrefix, String prevTablet) throws IOException{
      this.tabletPrefix = tabletPrefix;
      this.currentValue = null;
      this.currentIterator = getNextListOfTablets(prevTablet).iterator();
    }

    @Override
    public boolean hasNext() {
      if(!currentIterator.hasNext() && currentValue != null) {
        try {
          currentIterator =
              getNextListOfTablets(currentValue.getTabletName()).iterator();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
      return currentIterator.hasNext();
    }

    @Override
    public OzoneTablet next() {
      if(hasNext()) {
        currentValue = currentIterator.next();
        return currentValue;
      }
      throw new NoSuchElementException();
    }

    /**
     * Gets the next set of tablet list using proxy.
     * @param prevTablet
     * @return {@code List<OzoneTablet>}
     */
    private List<OzoneTablet> getNextListOfTablets(String prevTablet) throws
        IOException {
      return proxy.listTablets(databaseName, tableName, partitionName, tabletPrefix, prevTablet,
          listCacheSize);
    }
  }
}
