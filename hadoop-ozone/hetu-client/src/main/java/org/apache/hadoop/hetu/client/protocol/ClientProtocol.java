/*
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

package org.apache.hadoop.hetu.client.protocol;

import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hetu.client.DatabaseArgs;
import org.apache.hadoop.hetu.client.OzoneDatabase;
import org.apache.hadoop.hetu.client.OzonePartition;
import org.apache.hadoop.hetu.client.OzoneTable;
import org.apache.hadoop.hetu.client.OzoneTablet;
import org.apache.hadoop.hetu.client.OzoneTabletDetails;
import org.apache.hadoop.hetu.client.PartitionArgs;
import org.apache.hadoop.hetu.client.TableArgs;
import org.apache.hadoop.hetu.client.io.HetuInputStream;
import org.apache.hadoop.hetu.client.io.HetuOutputStream;
import org.apache.hadoop.hetu.client.io.TabletInputStream;
import org.apache.hadoop.hetu.client.io.TabletOutputStream;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmPartitionArgs;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmTabletInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneTabletStatus;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRoleInfo;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.ozone.security.auth.HetuObj;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.token.Token;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;

/**
 * An implementer of this interface is capable of connecting to Ozone Cluster
 * and perform client operations. The protocol used for communication is
 * determined by the implementation class specified by
 * property <code>ozone.client.protocol</code>. The build-in implementation
 * includes: {@link org.apache.hadoop.hetu.client.rpc.RpcClient} for RPC.
 */
@KerberosInfo(serverPrincipal = OMConfigKeys.OZONE_OM_KERBEROS_PRINCIPAL_KEY)
public interface ClientProtocol {

  /**
   * List of OM node Ids and their Ratis server roles.
   * @return List of OM server roles
   * @throws IOException
   */
  List<OMRoleInfo> getOmRoleInfos() throws IOException;

  void createDatabase(String databaseName, DatabaseArgs databaseArgs) throws IOException;

  void createDatabase(String databaseName) throws IOException;

  OzoneDatabase getDatabaseDetails(String databaseName) throws IOException;

  void deleteDatabase(String databaseName) throws IOException;

  /**
   * Checks if a Database exists and the user with a role specified has access
   * to the Database.
   * @param databaseName Name of the Database
   * @param auth requested auths which needs to be checked for access
   * @return Boolean - True if the user with a role can access the database.
   * This is possible for owners of the volume and admin users
   * @throws IOException
   */
  @Deprecated
  boolean checkDatabaseAccess(String databaseName, OzoneAcl auth)
          throws IOException;

  /**
   * Sets the owner of database.
   * @param databaseName Name of the Database
   * @param owner to be set for the Database
   * @return true if operation succeeded, false if specified user is
   *         already the owner.
   * @throws IOException
   */
  boolean setDatabaseOwner(String databaseName, String owner) throws IOException;

  /**
   * Set Database Quota.
   * @param databaseName Name of the Database
   * @param quotaInNamespace The maximum number of tables in this database.
   * @param quotaInBytes The maximum size this database can be used.
   * @throws IOException
   */
  void setDatabaseQuota(String databaseName, long quotaInNamespace,
                        long quotaInBytes) throws IOException;

  List<OzoneDatabase> listDatabase(String user, String databasePrefix, String prevDatabase, int maxListResult) throws IOException;;

  List<OzoneDatabase> listDatabase(String databasePrefix, String prevDatabase, int maxListResult) throws IOException;

  void renameDatabase(String fromDatabaseName, String toDatabaseName);

  /**
   * Creates a new Table in the Database.
   * @param databaseName Name of the Database
   * @param tableName Name of the Table
   * @throws IOException
   */
  void createTable(String databaseName, String tableName)
      throws IOException;

  /**
   * Creates a new Table in the Database, with properties set in TableArgs.
   * @param databaseName Name of the Database
   * @param tableName Name of the Table
   * @param tableArgs Table Arguments
   * @throws IOException
   */
  void createTable(String databaseName, String tableName,
                    TableArgs tableArgs)
      throws IOException;


  /**
   * Enables or disables Table Versioning.
   * @param databaseName Name of the Database
   * @param tableName Name of the Table
   * @param versioning True to enable Versioning, False to disable.
   * @throws IOException
   */
  void setTableVersioning(String databaseName, String tableName,
                           Boolean versioning)
      throws IOException;

  /**
   * Sets the Storage Class of a Table.
   * @param databaseName Name of the Database
   * @param tableName Name of the Table
   * @param storageType StorageType to be set
   * @throws IOException
   */
  void setTableStorageType(String databaseName, String tableName,
                            StorageType storageType)
      throws IOException;

  /**
   * Set Table Quota.
   * @param databaseName Name of the Database.
   * @param tableName Name of the Table.
   * @param quotaInBucket The maximum number of buckets in this table.
   * @param quotaInBytes The maximum size this tables can be used.
   * @throws IOException
   */
  void setTableQuota(String databaseName, String tableName,
                     int quotaInBucket, long quotaInBytes) throws IOException;

  /**
   * Deletes a table if it is empty.
   * @param databaseName Name of the Database
   * @param tableName Name of the Table
   * @throws IOException
   */
  void deleteTable(String databaseName, String tableName)
      throws IOException;

  /**
   * Returns {@link OzonePartition}.
   * @param databaseName Name of the Database
   * @param tableName Name of the Table
   * @return {@link OzonePartition}
   * @throws IOException
   */
  OzoneTable getTableDetails(String databaseName, String tableName)
      throws IOException;

  /**
   * Returns the List of Tables in the Database that matches the tablePrefix,
   * size of the returned list depends on maxListResult. The caller has to make
   * multiple calls to read all databases.
   * @param databaseName Name of the Database
   * @param tablePrefix Table prefix to match
   * @param prevTable Starting point of the list, this table is excluded
   * @param maxListResult Max number of buckets to return.
   * @return {@code List<OzoneTable>}
   * @throws IOException
   */
  List<OzoneTable> listTables(String databaseName, String tablePrefix,
                                  String prevTable, int maxListResult)
      throws IOException;


  /**
   * Creates a new Partition.
   * @param databaseName Name of the Database
   * @param tableName Name of the Table
   * @param partitionName Name of the Partition
   * @throws IOException
   */
  void createPartition(String databaseName, String tableName,
                       String partitionName)
          throws IOException;

  /**
   * Creates a new Partition with properties set in PartitionArgs.
   * @param databaseName Name of the Database
   * @param tableName Name of the Table
   * @param partitionName Name of the Partition
   * @param args Properties to be set for the Partition
   * @throws IOException
   */
  void createPartition(String databaseName, String tableName,
                       String partitionName, PartitionArgs args)
          throws IOException;

  /**
   * Sets the omPartitionArgs of partition.
   * @param omPartitionArgs partitionArg of partition
   * @return true if operation succeeded, false if specified user is
   *         already the owner.
   * @throws IOException
   */
  public void setPartitionProperty(OmPartitionArgs omPartitionArgs)
          throws IOException;

  /**
   * Returns {@link OzonePartition}.
   * @param databaseName Name of the Database
   * @param tableName Name of the Table
   * @param partitionName Name of the Partition
   * @return {@link OzonePartition}
   * @throws IOException
   * */
  OzonePartition getPartitionDetails(String databaseName, String tableName, String partitionName)
          throws IOException;

  /**
   * Deletes an empty Partition.
   * @param databaseName Name of the Database
   * @param tableName Name of the Table
   * @param partitionName Name of the Partition
   * @throws IOException
   *
   */
  void deletePartition(String databaseName, String tableName,
                       String partitionName) throws IOException;

  /**
   * Lists all partitions in the cluster that are owned by the specified
   * user and matches the partitionPrefix, size of the returned list depends on
   * maxListResult. If the user is null, return partitions owned by current table.
   * If partition prefix is null, returns all the partitions. The caller has to make
   * multiple calls to read all partitions.
   *
   * @param databaseName Name of the Database
   * @param tableName Name of the Table
   * @param partitionPrefix Partition prefix to match
   * @param prevPartition Starting point of the list, this partition is excluded
   * @param maxListResult Max number of partitions to return.
   * @return {@code List<OzonePartition>}
   * @throws IOException
   */
  List<OzonePartition> listPartitions(String databaseName, String tableName, String partitionPrefix,
                                      String prevPartition, int maxListResult)
          throws IOException;

  /**
   * Writes a tablet in an existing partition.
   * @param databaseName Name of the Database
   * @param tableName Name of the Table
   * @param partitionName Name of the Partition
   * @param tabletName Name of the Tablet
   * @param size Size of the data
   * @param metadata custom key value metadata
   * @return {@link TabletOutputStream}
   *
   */
  @Deprecated
  HetuOutputStream createTablet(String databaseName, String tableName, String partitionName,
                                  String tabletName, long size, ReplicationType type,
                                  ReplicationFactor factor,
                                  Map<String, String> metadata)
      throws IOException;

  /**
   * Writes a tablet in an existing table.
   * @param databaseName Name of the Database
   * @param tableName Name of the Table
   * @param partitionName Name of the Partition
   * @param tabletName Name of the Tablet
   * @param size Size of the data
   * @param metadata custom key value metadata
   * @return {@link TabletOutputStream}
   *
   */
  HetuOutputStream createTablet(String databaseName, String tableName, String partitionName,
                                String tabletName, long size, ReplicationConfig replicationConfig,
                                Map<String, String> metadata)
      throws IOException;


  /**
   * Reads a tablet from an existing table.
   * @param databaseName Name of the Database
   * @param tableName Name of the Table
   * @param partitionName Name of the Partition
   * @param tabletName Name of the Tablet
   * @return {@link TabletInputStream}
   * @throws IOException
   */
  HetuInputStream getTablet(String databaseName, String tableName, String partitionName,
      String tabletName)
      throws IOException;


  /**
   * Deletes an existing tablet.
   * @param databaseName Name of the Database
   * @param tableName Name of the Table
   * @param partitionName Name of the Partition
   * @param tabletName Name of the Tablet
   * @throws IOException
   */
  void deleteTablet(String databaseName, String tableName, String partitionName,
      String tabletName)
      throws IOException;

  /**
   * Deletes tablets through the list.
   * @param databaseName Name of the Database
   * @param tableName Name of the Table
   * @param partitionName name of the Partition
   * @param tabletNameList List of the Tablet
   * @throws IOException
   */
  void deleteTablets(String databaseName, String tableName, String partitionName,
                  List<String> tabletNameList)
      throws IOException;

  /**
   * Renames an existing table within a database.
   * @param databaseName Name of the Database
   * @param fromTableName Name of the table to be renamed
   * @param toTableName New name to be used for the table
   * @throws IOException
   */
  void renameTable(String databaseName, String fromTableName,
                 String toTableName) throws IOException;

  /**
   * Renames existing tables within a database.
   * @param databaseName Name of the Database
   * @param tableMap The table is original table name nad value is new table name.
   * @throws IOException
   */
  void renameTables(String databaseName,
                  Map<String, String> tableMap) throws IOException;

  /**
   * Returns list of tablets in {Database/Table/Partition} that matches the tabletPrefix,
   * size of the returned list depends on maxListResult. The caller has
   * to make multiple calls to read all tablets.
   * @param databaseName Name of the Database
   * @param tableName Name of the Table
   * @param partitionName Name of the Partition
   * @param tabletPrefix Partition prefix to match
   * @param prevTablet Starting point of the list, this Tablet is excluded
   * @param maxListResult Max number of Partitions to return.
   * @return {@code List<OzoneTablet>}
   * @throws IOException
   */
  List<OzoneTablet> listTablets(String databaseName, String tableName, String partitionName,
                                String tabletPrefix, String prevTablet, int maxListResult)
      throws IOException;

  /**
   * List trash allows the user to list the tablets that were marked as deleted,
   * but not actually deleted by Ozone Manager. This allows a user to recover
   * tablets within a configurable window.
   * @param databaseName - The database name, which can also be a wild card
   *                   using '*'.
   * @param tableName - The table name, which can also be a wild card
   *                   using '*'.
   * @param partitionName - The partition name, which can also be a wild card
   *                   using '*'.
   * @param startTabletName - List tablets form a specific table name.
   * @param tabletPrefix - List tablets using a specific prefix.
   * @param maxTablets - The number of tablets to be returned. This must be below
   *                the cluster level set by admins.
   * @return The list of tablets that are deleted from the deleted table.
   * @throws IOException
   */
  List<RepeatedOmTabletInfo> listTrash(String databaseName, String tableName, String partitionName,
                                       String startTabletName, String tabletPrefix,
                                       int maxTablets)
      throws IOException;

  /**
   * Recover trash allows the user to recover tablets that were marked as deleted,
   * but not actually deleted by Ozone Manager.
   * @param databaseName - The database name.
   * @param tableName - The table name.
   * @param partitionName - The partition name.
   * @param tabletName - The tablet user want to recover.
   * @param destinationPartition - The partition user want to recover to.
   * @return The result of recovering operation is success or not.
   * @throws IOException
   */
  boolean recoverTrash(String databaseName, String tableName, String partitionName,
      String tabletName, String destinationPartition) throws IOException;

  /**
   * Get OzoneTablet.
   * @param databaseName Name of the Database
   * @param tableName Name of the Table
   * @param partitionName Name of the Partition
   * @param tabletName Tablet name
   * @return {@link OzoneTablet}
   * @throws IOException
   */
  OzoneTabletDetails getTabletDetails(String databaseName, String tableName,
                                      String partitionName, String tabletName)
      throws IOException;

  /**
   * Close and release the resources.
   */
  void close() throws IOException;

  /**
   * Get a valid Delegation Token.
   *
   * @param renewer the designated renewer for the token
   * @return Token<OzoneDelegationTokenSelector>
   * @throws IOException
   */
  Token<OzoneTokenIdentifier> getDelegationToken(Text renewer)
      throws IOException;

  /**
   * Renew an existing delegation token.
   *
   * @param token delegation token obtained earlier
   * @return the new expiration time
   * @throws IOException
   */
  long renewDelegationToken(Token<OzoneTokenIdentifier> token)
      throws IOException;

  /**
   * Cancel an existing delegation token.
   *
   * @param token delegation token
   * @throws IOException
   */
  void cancelDelegationToken(Token<OzoneTokenIdentifier> token)
      throws IOException;

  /**
   * Get KMS client provider.
   * @return KMS client provider.
   * @throws IOException
   */
  KeyProvider getKeyProvider() throws IOException;

  /**
   * Get KMS client provider uri.
   * @return KMS client provider uri.
   * @throws IOException
   */
  URI getKeyProviderUri() throws IOException;

  /**
   * Get CanonicalServiceName for ozone delegation token.
   * @return Canonical Service Name of ozone delegation token.
   */
  String getCanonicalServiceName();

  /**
   * Get the Ozone Tablet Status for a particular Ozone tablet.
   *
   * @param databaseName Database name.
   * @param tableName Table name.
   * @param partitionName Partition name.
   * @param tabletName    Tablet name.
   * @return OzoneTabletStatus for the tablet.
   * @throws OMException if tablet does not exist
   *                     if partition does not exist
   * @throws IOException if there is error in the db
   *                     invalid arguments
   */
  OzoneTabletStatus getOzoneTabletStatus(String databaseName, String tableName,
      String partitionName, String tabletName) throws IOException;

  /**
   * Creates an input stream for reading tablet contents.
   *
   * @param databaseName Database name
   * @param tableName Table name
   * @param partitionName Partition name
   * @param tabletName    Absolute path of the tablet to be read
   * @return Input stream for reading the tablet
   * @throws OMException if any entry in the path exists as a tablet
   *                     if partition does not exist
   * @throws IOException if there is error in the db
   *                     invalid arguments
   */
  HetuInputStream readTablet(String databaseName, String tableName,
                             String partitionName, String tabletName) throws IOException;

  /**
   * Creates an output stream for writing to a tablet.
   *
   * @param databaseName Database name
   * @param tableName Table name
   * @param partitionName Partition name
   * @param tabletName    Absolute path of the tablet to be written
   * @param size       Size of data to be written
   * @param type       Replication Type
   * @param factor     Replication Factor
   * @param overWrite  if true existing tablet at the location will be overwritten
   * @param recursive  if true tablet would be created even if parent directories
   *                   do not exist
   * @return Output stream for writing to the tablet
   * @throws OMException if given tablet is a directory
   *                     if tablet exists and isOverwrite flag is false
   *                     if an ancestor exists as a tablet
   *                     if partition does not exist
   * @throws IOException if there is error in the db
   *                     invalid arguments
   */
  @SuppressWarnings("checkstyle:parameternumber")
  HetuOutputStream createTablet(String databaseName, String tableName, String partitionName,
      String tabletName, long size, ReplicationType type, ReplicationFactor factor,
      boolean overWrite, boolean recursive) throws IOException;


  /**
   * Creates an output stream for writing to a tablet
   *
   * @param databaseName Database Name
   * @param tableName Table name
   * @param partitionName Partition Name
   * @param tabletName    Absolute path of the tablet to be written
   * @param size       Size of data to be written
   * @param replicationConfig Replication config
   * @param overWrite  if true existing tablet at the location will be overwritten
   * @param recursive  if true tablet would be created even if parent directories
   *                   do not exist
   * @return Output stream for writing to the tablet
   * @throws OMException if given tablet is a directory
   *                     if tablet exists and isOverwrite flag is false
   *                     if an ancestor exists as a tablet
   *                     if partition does not exist
   * @throws IOException if there is error in the db
   *                     invalid arguments
   */
  @SuppressWarnings("checkstyle:parameternumber")
  HetuOutputStream createTablet(String databaseName, String tableName,
      String partitionName, String tabletName, long size, ReplicationConfig replicationConfig,
      boolean overWrite, boolean recursive) throws IOException;


  /**
   * List the status for a tablet or a directory and its contents.
   *
   * @param databaseName Database name
   * @param tableName Table Name
   * @param partitionName Partition Name
   * @param tabletName    Absolute path of the entry to be listed
   * @param recursive  For a directory if true all the descendants of a
   *                   particular directory are listed
   * @param startTablet   Tablet from which listing needs to start. If startTablet exists
   *                   its status is included in the final list.
   * @param numEntries Number of entries to list from the start tablet
   * @return list of file status
   */
  List<OzoneTabletStatus> listStatus(String databaseName, String tableName, String partitionName,
      String tabletName, boolean recursive, String startTablet, long numEntries)
      throws IOException;


  /**
   * Add auth for Ozone object. Return true if auth is added successfully else
   * false.
   * @param obj Ozone object for which auth should be added.
   * @param auth ozone auth to be added.
   *
   * @throws IOException if there is error.
   * */
  boolean addAuth(HetuObj obj, OzoneAcl auth) throws IOException;

  /**
   * Remove auth for Ozone object. Return true if auth is removed successfully
   * else false.
   * @param obj Ozone object.
   * @param auth Ozone auth to be removed.
   *
   * @throws IOException if there is error.
   * */
  boolean removeAuth(HetuObj obj, OzoneAcl auth) throws IOException;

  /**
   * Auths to be set for given Ozone object. This operations reset AUTH for
   * given object to list of auths provided in argument.
   * @param obj Ozone object.
   * @param auths List of auths.
   *
   * @throws IOException if there is error.
   * */
  boolean setAuth(HetuObj obj, List<OzoneAcl> auths) throws IOException;

  /**
   * Returns list of Auths for given Ozone object.
   * @param obj Ozone object.
   *
   * @throws IOException if there is error.
   * */
  List<OzoneAcl> getAuth(HetuObj obj) throws IOException;

  /**
   * Getter for OzoneManagerClient.
   */
  OzoneManagerProtocol getOzoneManagerClient();

  HetuOutputStream openTablet(String databaseName, String tableName,
                              String partitionName, String tabletName, long size,
                              ReplicationConfig replicationConfig) throws IOException;
}
