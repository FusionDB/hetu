/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.DBStoreHAManager;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.hm.HmDatabaseArgs;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmPartitionInfo;
import org.apache.hadoop.ozone.om.helpers.OmPrefixInfo;
import org.apache.hadoop.ozone.om.helpers.OmTableInfo;
import org.apache.hadoop.ozone.om.helpers.OmTabletInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmTabletInfo;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.om.lock.OzoneManagerLock;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.ozone.storage.proto.
    OzoneManagerStorageProtos.PersistedUserDatabaseInfo;
import org.apache.hadoop.ozone.storage.proto.
    OzoneManagerStorageProtos.PersistedUserVolumeInfo;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.Table;

import com.google.common.annotations.VisibleForTesting;

/**
 * OM metadata manager interface.
 */
public interface OMMetadataManager extends DBStoreHAManager {
  /**
   * Start metadata manager.
   *
   * @param configuration
   * @throws IOException
   */
  void start(OzoneConfiguration configuration) throws IOException;

  /**
   * Stop metadata manager.
   */
  void stop() throws Exception;

  /**
   * Get metadata store.
   *
   * @return metadata store.
   */
  @VisibleForTesting
  DBStore getStore();

  /**
   * Returns the OzoneManagerLock used on Metadata DB.
   *
   * @return OzoneManagerLock
   */
  OzoneManagerLock getLock();

  /**
   * Returns the epoch associated with current OM process.
   */
  long getOmEpoch();

  /**
   * Given a volume return the corresponding DB key.
   *
   * @param volume - Volume name
   */
  String getVolumeKey(String volume);

  /**
   * Given a user return the corresponding DB key.
   *
   * @param user - User name
   */
  String getUserKey(String user);

  /**
   * Given a volume and bucket, return the corresponding DB key.
   *
   * @param volume - User name
   * @param bucket - Bucket name
   */
  String getBucketKey(String volume, String bucket);

  /**
   * Given a volume, bucket and a key, return the corresponding DB key.
   *
   * @param volume - volume name
   * @param bucket - bucket name
   * @param key    - key name
   * @return DB key as String.
   */
  String getOzoneKey(String volume, String bucket, String key);

  /**
   * Given a volume, bucket and a key, return the corresponding DB key.
   *
   * @param database - database name
   * @param table - table name
   * @param partition - partition name
   * @param tablet    - tablet name
   * @return DB key as String.
   */
  String getOzoneTablet(String database, String table, String partition, String tablet);

  /**
   * Given a volume, bucket and a key, return the corresponding DB directory
   * key.
   *
   * @param volume - volume name
   * @param bucket - bucket name
   * @param key    - key name
   * @return DB directory key as String.
   */
  String getOzoneDirKey(String volume, String bucket, String key);

  /**
   * Given a database, table and a tablet, return the corresponding DB directory
   * key.
   *
   * @param database - database name
   * @param table - table name
   * @param partition - partition name
   * @param tablet    - tablet name
   * @return DB directory key as String.
   */
  String getOzoneDirTablet(String database, String table, String partition, String tablet);


  /**
   * Returns the DB key name of a open key in OM metadata store. Should be
   * #open# prefix followed by actual key name.
   *
   * @param volume - volume name
   * @param bucket - bucket name
   * @param key - key name
   * @param id - the id for this open
   * @return bytes of DB key.
   */
  String getOpenKey(String volume, String bucket, String key, long id);

  /**
   * Returns the DB key name of a open tablet key in OM metadata store. Should be
   * #open# prefix followed by actual tablet key name.
   *
   * @param database - database name
   * @param table - table name
   * @param partition - partition name
   * @param tablet - tablet name
   * @param id - the id for this open
   * @return bytes of DB tablet key.
   */
  String getOpenTablet(String database, String table, String partition, String tablet, long id);

  /**
   * Given a volume, check if it is empty, i.e there are no buckets inside it.
   *
   * @param volume - Volume name
   */
  boolean isVolumeEmpty(String volume) throws IOException;

  /**
   * Given a volume/bucket, check if it is empty, i.e there are no keys inside
   * it.
   *
   * @param volume - Volume name
   * @param bucket - Bucket name
   * @return true if the bucket is empty
   */
  boolean isBucketEmpty(String volume, String bucket) throws IOException;

  /**
   * Returns a list of buckets represented by {@link OmBucketInfo} in the given
   * volume.
   *
   * @param volumeName the name of the volume. This argument is required, this
   * method returns buckets in this given volume.
   * @param startBucket the start bucket name. Only the buckets whose name is
   * after this value will be included in the result. This key is excluded from
   * the result.
   * @param bucketPrefix bucket name prefix. Only the buckets whose name has
   * this prefix will be included in the result.
   * @param maxNumOfBuckets the maximum number of buckets to return. It ensures
   * the size of the result will not exceed this limit.
   * @return a list of buckets.
   * @throws IOException
   */
  List<OmBucketInfo> listBuckets(String volumeName, String startBucket,
      String bucketPrefix, int maxNumOfBuckets)
      throws IOException;

  /**
   * Returns a list of keys represented by {@link OmKeyInfo} in the given
   * bucket.
   *
   * @param volumeName the name of the volume.
   * @param bucketName the name of the bucket.
   * @param startKey the start key name, only the keys whose name is after this
   * value will be included in the result. This key is excluded from the
   * result.
   * @param keyPrefix key name prefix, only the keys whose name has this prefix
   * will be included in the result.
   * @param maxKeys the maximum number of keys to return. It ensures the size of
   * the result will not exceed this limit.
   * @return a list of keys.
   * @throws IOException
   */
  List<OmKeyInfo> listKeys(String volumeName,
      String bucketName, String startKey, String keyPrefix, int maxKeys)
      throws IOException;

  /**
   * List trash allows the user to list the keys that were marked as deleted,
   * but not actually deleted by Ozone Manager. This allows a user to recover
   * keys within a configurable window.
   * @param volumeName - The volume name, which can also be a wild card
   *                   using '*'.
   * @param bucketName - The bucket name, which can also be a wild card
   *                   using '*'.
   * @param startKeyName - List keys from a specific key name.
   * @param keyPrefix - List keys using a specific prefix.
   * @param maxKeys - The number of keys to be returned. This must be below
   *                the cluster level set by admins.
   * @return The list of keys that are deleted from the deleted table.
   * @throws IOException
   */
  List<RepeatedOmKeyInfo> listTrash(String volumeName, String bucketName,
      String startKeyName, String keyPrefix, int maxKeys) throws IOException;

  /**
   * Recover trash allows the user to recover the keys
   * that were marked as deleted, but not actually deleted by Ozone Manager.
   * @param volumeName - The volume name.
   * @param bucketName - The bucket name.
   * @param keyName - The key user want to recover.
   * @param destinationBucket - The bucket user want to recover to.
   * @return The result of recovering operation is success or not.
   */
  boolean recoverTrash(String volumeName, String bucketName,
      String keyName, String destinationBucket) throws IOException;

  /**
   * Returns a list of volumes owned by a given user; if user is null, returns
   * all volumes.
   *
   * @param userName volume owner
   * @param prefix the volume prefix used to filter the listing result.
   * @param startKey the start volume name determines where to start listing
   * from, this key is excluded from the result.
   * @param maxKeys the maximum number of volumes to return.
   * @return a list of {@link OmVolumeArgs}
   * @throws IOException
   */
  List<OmVolumeArgs> listVolumes(String userName, String prefix,
      String startKey, int maxKeys) throws IOException;

  /**
   * Returns a list of pending deletion key info that ups to the given count.
   * Each entry is a {@link BlockGroup}, which contains the info about the key
   * name and all its associated block IDs. A pending deletion key is stored
   * with #deleting# prefix in OM DB.
   *
   * @param count max number of keys to return.
   * @return a list of {@link BlockGroup} represent keys and blocks.
   * @throws IOException
   */
  List<BlockGroup> getPendingDeletionKeys(int count) throws IOException;

  /**
   * Returns the names of up to {@code count} open keys that are older than
   * the configured expiration age.
   *
   * @param count The maximum number of open keys to return.
   * @return a list of {@link String} representing names of open expired keys.
   * @throws IOException
   */
  List<String> getExpiredOpenKeys(int count) throws IOException;

  /**
   * Returns the user Table.
   *
   * @return UserTable.
   */
  Table<String, PersistedUserVolumeInfo> getUserTable();

  /**
   * Returns the user Table.
   *
   * @return UserTable.
   */
  Table<String, PersistedUserDatabaseInfo> getUserTableDb();

  /**
   * Returns the Volume Table.
   *
   * @return VolumeTable.
   */
  Table<String, OmVolumeArgs> getVolumeTable();

  /**
   * Returns the BucketTable.
   *
   * @return BucketTable.
   */
  Table<String, OmBucketInfo> getBucketTable();

  /**
   * Returns the KeyTable.
   *
   * @return KeyTable.
   */
  Table<String, OmKeyInfo> getKeyTable();

  /**
   * Get Deleted Table.
   *
   * @return Deleted Table.
   */
  Table<String, RepeatedOmKeyInfo> getDeletedTable();

  /**
   * Get Deleted tablet
   * @return Deleted Tablet Table.
   */
  Table<String, RepeatedOmTabletInfo> getDeletedTablet();

  /**
   * Gets the OpenKeyTable.
   *
   * @return Table.
   */
  Table<String, OmKeyInfo> getOpenKeyTable();

  /**
   * Gets the OpenTabletTable.
   *
   * @return Table.
   */
  Table<String, OmTabletInfo> getOpenTabletTable();

  /**
   * Gets the DelegationTokenTable.
   *
   * @return Table.
   */
  Table<OzoneTokenIdentifier, Long> getDelegationTokenTable();

  /**
   * Gets the Ozone prefix path to its acl mapping table.
   * @return Table.
   */
  Table<String, OmPrefixInfo> getPrefixTable();

  /**
   * Returns the DB key name of a multipart upload key in OM metadata store.
   *
   * @param volume - volume name
   * @param bucket - bucket name
   * @param key - key name
   * @param uploadId - the upload id for this key
   * @return bytes of DB key.
   */
  String getMultipartKey(String volume, String bucket, String key, String
      uploadId);

  /**
   * Gets the multipart info table which holds the information about
   * multipart upload information of the keys.
   * @return Table
   */
  Table<String, OmMultipartKeyInfo> getMultipartInfoTable();

  /**
   * Gets the S3 Secrets table.
   * @return Table
   */
  Table<String, S3SecretValue> getS3SecretTable();

  Table<String, TransactionInfo> getTransactionInfoTable();

  /**
   * Returns number of rows in a table.  This should not be used for very
   * large tables.
   * @param table
   * @return long
   * @throws IOException
   */
  <KEY, VALUE> long countRowsInTable(Table<KEY, VALUE> table)
      throws IOException;

  /**
   * Returns an estimated number of rows in a table.  This is much quicker
   * than {@link OMMetadataManager#countRowsInTable} but the result can be
   * inaccurate.
   * @param table Table
   * @return long Estimated number of rows in the table.
   * @throws IOException
   */
  <KEY, VALUE> long countEstimatedRowsInTable(Table<KEY, VALUE> table)
      throws IOException;

  /**
   * Return the existing upload keys which includes volumeName, bucketName,
   * keyName.
   */
  Set<String> getMultipartUploadKeys(String volumeName,
      String bucketName, String prefix) throws IOException;

  /**
   * Return table mapped to the specified table name.
   * @param tableName
   * @return Table
   */
  Table getTable(String tableName);

  /**
   * Return a map of tableName and table in OM DB.
   * @return map of table and table name.
   */
  Map<String, Table> listTables();

  /**
   * Return Set of table names created in OM DB.
   * @return table names in OM DB.
   */
  Set<String> listTableNames();

  Iterator<Map.Entry<CacheKey<String>, CacheValue<OmBucketInfo>>>
      getBucketIterator();

  TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
      getKeyIterator();

  /**
   * Returns the Database Table.
   *
   * @return DatabaseTable.
   */
  Table<String, HmDatabaseArgs> getDatabaseTable();

  /**
   * Given a database return the corresponding DB key.
   *
   * @param database - Database name
   */
  String getDatabaseKey(String database);

  /**
   * Given a database, check if it is empty, i.e there are no tables inside it.
   *
   * @param database - Database name
   */
  boolean isDatabaseEmpty(String database) throws IOException;

  /**
   * Returns a list of databases owned by a given user; if user is null, returns
   * all databases.
   *
   * @param userName database owner
   * @param prefix the database prefix used to filter the listing result.
   * @param startKey the start database name determines where to start listing
   * from, this key is excluded from the result.
   * @param maxKeys the maximum number of databases to return.
   * @return a list of {@link HmDatabaseArgs}
   * @throws IOException
   */
  List<HmDatabaseArgs> listDatabase(String userName, String prefix, String startKey, int maxKeys) throws IOException;

  /**
   * Given a table return the corresponding DB key.
   *
   *  @param databaseName - database name
   * @param tableName - table name
   */
  String getMetaTableKey(String databaseName, String tableName);

  /**
   * Given a table return the corresponding DB key.
   *
   *  @param databaseName - database name
   * @param tableName - table name
   * @param partitionName - partition name
   */
  String getPartitionKey(String databaseName, String tableName, String partitionName);

  /**
   * Returns the Meta Table.
   * @return
   */
  Table<String, OmTableInfo> getMetaTable();

  /**
   * Returns the Partition Table.
   * @return
   */
  Table<String, OmPartitionInfo> getPartitionTable();

  /**
   * Returns the tablet Table.
   * @return
   */
  Table<String, OmTabletInfo> getTabletTable();

  /**
   * Given a table, check if it is empty, i.e there are no tablets inside it.
   * @param databaseName
   * @param tableName
   * @return
   */
  boolean isMetaTableEmpty(String databaseName, String tableName) throws IOException;

  /**
   * Given a table, check if it is empty, i.e there are no tablets inside it.
   * @param databaseName
   * @param tableName
   * @param partitionName
   * @return
   */
  boolean isPartitionEmpty(String databaseName, String tableName, String partitionName) throws IOException;

  /**
   * Returns a list of tables represented by {@link OmTableInfo} in the given
   * databse.
   *
   * @param databaseName the name of the database. This argument is required, this
   * method returns tables in this given database.
   * @param startTable the start table name. Only the tables whose name is
   * after this value will be included in the result. This key is excluded from
   * the result.
   * @param tablePrefix table name prefix. Only the tables whose name has
   * this prefix will be included in the result.
   * @param maxNumOfTables the maximum number of tables to return. It ensures
   * the size of the result will not exceed this limit.
   * @return a list of tables.
   * @throws IOException
   */
  List<OmTableInfo> listMetaTables(String databaseName, String startTable,
                                 String tablePrefix, int maxNumOfTables)
          throws IOException;

  /**
   * Returns a list of tables represented by {@link OmTableInfo} in the given
   * databse.
   *
   * @param databaseName the name of the database. This argument is required, this
   * method returns tables in this given database.
   * @param tableName the name of the meta table. This argument is required, this
   * method returns partitions in this given meta table.
   * @param startPartition the start partition name. Only the tables whose name is
   * after this value will be included in the result. This key is excluded from
   * the result.
   * @param partitionPrefix partition name prefix. Only the tables whose name has
   * this prefix will be included in the result.
   * @param maxNumOfTables the maximum number of partitions to return. It ensures
   * the size of the result will not exceed this limit.
   * @return a list of tables.
   * @throws IOException
   */
  List<OmPartitionInfo> listPartitions(String databaseName, String tableName, String startPartition,
                                   String partitionPrefix, int maxNumOfTables)
          throws IOException;
}
