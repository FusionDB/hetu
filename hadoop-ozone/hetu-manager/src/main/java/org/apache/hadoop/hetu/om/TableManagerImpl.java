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
package org.apache.hadoop.hetu.om;

import com.google.common.base.Preconditions;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.hetu.hm.helpers.OmDatabaseArgs;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmTableArgs;
import org.apache.hadoop.ozone.om.helpers.OmTableInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneAclUtil;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.RequestContext;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.BUCKET_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INTERNAL_ERROR;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.TABLE_NOT_FOUND;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.DATABASE_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.TABLE_LOCK;

/**
 * OM table manager.
 */
public class TableManagerImpl implements TableManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(TableManagerImpl.class);

  /**
   * OMMetadataManager is used for accessing OM MetadataDB and ReadWriteLock.
   */
  private final OMMetadataManager metadataManager;
  private final KeyProviderCryptoExtension kmsProvider;

  /**
   * Constructs tableManager.
   *
   * @param metadataManager
   */
  public TableManagerImpl(OMMetadataManager metadataManager) {
    this(metadataManager, null, false);
  }

  public TableManagerImpl(OMMetadataManager metadataManager,
                          KeyProviderCryptoExtension kmsProvider) {
    this(metadataManager, kmsProvider, false);
  }

  public TableManagerImpl(OMMetadataManager metadataManager,
                          KeyProviderCryptoExtension kmsProvider, boolean isRatisEnabled) {
    this.metadataManager = metadataManager;
    this.kmsProvider = kmsProvider;
  }

  KeyProviderCryptoExtension getKMSProvider() {
    return kmsProvider;
  }

  /**
   * MetadataDB is maintained in MetadataManager and shared between
   * TableManager and DatabaseManager. (and also by BlockManager)
   *
   * TableManager uses MetadataDB to store table level information.
   *
   * Keys used in TableManager for storing data into MetadataDB
   * for TableInfo:
   * {database/table} -> tableInfo
   *
   * Work flow of create table:
   *
   * -> Check if the Database exists in metadataDB, if not throw
   * DatabaseNotFoundException.
   * -> Else check if the Table exists in metadataDB, if so throw
   * TableExistException
   * -> Else update MetadataDB with DatabaseInfo.
   */

  /**
   * Creates a table.
   *
   * @param tableInfo - OmTableInfo.
   */
  @Override
  public void createTable(OmTableInfo tableInfo) throws IOException {
    Preconditions.checkNotNull(tableInfo);
    String databaseName = tableInfo.getDatabaseName();
    String tableName = tableInfo.getTableName();
    boolean acquiredTableLock = false;
    metadataManager.getLock().acquireWriteLock(DATABASE_LOCK, databaseName);
    try {
      acquiredTableLock = metadataManager.getLock().acquireWriteLock(
          TABLE_LOCK, databaseName, tableName);
      String databaseKey = metadataManager.getDatabaseKey(databaseName);
      String tableKey = metadataManager.getMetaTableKey(databaseName, tableName);
      OmDatabaseArgs databaseArgs = metadataManager.getDatabaseTable().get(databaseKey);

      //Check if the database exists
      if (databaseArgs == null) {
        LOG.debug("database: {} not found ", databaseName);
        throw new OMException("Database doesn't exist",
            OMException.ResultCodes.DATABASE_NOT_FOUND);
      }
      //Check if table already exists
      if (metadataManager.getMetaTable().get(tableKey) != null) {
        LOG.debug("table: {} already exists ", tableName);
        throw new OMException("Table already exist",
            OMException.ResultCodes.TABLE_ALREADY_EXISTS);
      }

      OmTableInfo.Builder omTableInfoBuilder = tableInfo.toBuilder()
          .setCreationTime(Time.now());

      // TODO acl
//      OzoneAclUtil.inheritDefaultAcls(omTableInfoBuilder.getAcls(),
//          databaseArgs.getDefaultAcls());

      OmTableInfo omTableInfo = omTableInfoBuilder.build();
      commitTableInfoToDB(omTableInfo);

      LOG.debug("created table: {} in database: {}", tableName,
            databaseName);
    } catch (IOException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Table creation failed for table:{} in database:{}",
            tableName, databaseName, ex);
      }
      throw ex;
    } finally {
      if (acquiredTableLock) {
        metadataManager.getLock().releaseWriteLock(TABLE_LOCK, databaseName,
            tableName);
      }
      metadataManager.getLock().releaseWriteLock(DATABASE_LOCK, databaseName);
    }
  }

  private void commitTableInfoToDB(OmTableInfo omTableInfo)
          throws IOException {
    String dbTableKey =
            metadataManager.getMetaTableKey(omTableInfo.getDatabaseName(),
                    omTableInfo.getTableName());
    metadataManager.getMetaTable().put(dbTableKey,
            omTableInfo);
  }

  /**
   * Returns Table Information.
   *
   * @param databaseName - Name of the Database.
   * @param tableName - Name of the Table.
   */
  @Override
  public OmTableInfo getTableInfo(String databaseName, String tableName)
      throws IOException {
    Preconditions.checkNotNull(databaseName);
    Preconditions.checkNotNull(tableName);
    metadataManager.getLock().acquireReadLock(TABLE_LOCK, databaseName,
        tableName);
    try {
      String tableKey = metadataManager.getMetaTableKey(databaseName, tableName);
      OmTableInfo value = metadataManager.getMetaTable().get(tableKey);
      if (value == null) {
        LOG.debug("table: {} not found in database: {}.", tableName,
            databaseName);
        throw new OMException("Table not found",
            TABLE_NOT_FOUND);
      }
      return value;
    } catch (IOException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Exception while getting table info for table: {}",
            tableName, ex);
      }
      throw ex;
    } finally {
      metadataManager.getLock().releaseReadLock(TABLE_LOCK, databaseName,
          tableName);
    }
  }

  /**
   * Sets table property from args.
   *
   * @param args - TableArgs.
   * @throws IOException - On Failure.
   */
  @Override
  public void setTableProperty(OmTableArgs args) throws IOException {
    Preconditions.checkNotNull(args);
    String databaseName = args.getDatabaseName();
    String tableName = args.getTableName();
    metadataManager.getLock().acquireWriteLock(TABLE_LOCK, databaseName,
        tableName);
    try {
      String tableKey = metadataManager.getMetaTableKey(databaseName, tableName);
      OmTableInfo oldTableInfo =
          metadataManager.getMetaTable().get(tableKey);
      //Check if table exist
      if (oldTableInfo == null) {
        LOG.debug("table: {} not found ", tableName);
        throw new OMException("Table doesn't exist",
            TABLE_NOT_FOUND);
      }
      OmTableInfo.Builder tableInfoBuilder = OmTableInfo.newBuilder();
      tableInfoBuilder.setDatabaseName(oldTableInfo.getDatabaseName());
      tableInfoBuilder.setTableName(tableName);
      tableInfoBuilder.addAllMetadata(args.getMetadata());

      //Check StorageType to update
      StorageType storageType = args.getStorageType();
      if (storageType != null) {
        tableInfoBuilder.setStorageType(storageType);
        LOG.debug("Updating table storage type for table: {} in database: {}",
            tableName, databaseName);
      } else {
        tableInfoBuilder.setStorageType(oldTableInfo.getStorageType());
      }

      //Check Versioning to update
      Boolean versioning = args.getIsVersionEnabled();
      if (versioning != null) {
        tableInfoBuilder.setIsVersionEnabled(versioning);
        LOG.debug("Updating table versioning for table: {} in database: {}",
            tableName, databaseName);
      } else {
        tableInfoBuilder
            .setIsVersionEnabled(oldTableInfo.getIsVersionEnabled());
      }
      tableInfoBuilder.setCreationTime(oldTableInfo.getCreationTime());

      // Set acls from oldTableInfo if it has any.
//      if (oldTableInfo.getAcls() != null) {
//        tableInfoBuilder.setAcls(oldTableInfo.getAcls());
//      }

      OmTableInfo omTableInfo = tableInfoBuilder.build();

      commitTableInfoToDB(omTableInfo);
    } catch (IOException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Setting table property failed for table:{} in database:{}",
            tableName, databaseName, ex);
      }
      throw ex;
    } finally {
      metadataManager.getLock().releaseWriteLock(TABLE_LOCK, databaseName,
          tableName);
    }
  }

  /**
   * Deletes an existing empty table from database.
   *
   * @param databaseName - Name of the Database.
   * @param tableName - Name of the Table.
   * @throws IOException - on Failure.
   */
  @Override
  public void deleteTable(String databaseName, String tableName)
      throws IOException {
    Preconditions.checkNotNull(databaseName);
    Preconditions.checkNotNull(tableName);
    metadataManager.getLock().acquireWriteLock(TABLE_LOCK, databaseName,
        tableName);
    try {
      //Check if table exists
      String tableKey = metadataManager.getMetaTableKey(databaseName, tableName);
      if (metadataManager.getMetaTable().get(tableKey) == null) {
        LOG.debug("table: {} not found ", tableName);
        throw new OMException("Table doesn't exist",
            TABLE_NOT_FOUND);
      }
      //Check if table is empty
      if (!metadataManager.isMetaTableEmpty(databaseName, tableName)) {
        LOG.debug("table: {} is not empty ", tableName);
        throw new OMException("Table is not empty",
            OMException.ResultCodes.TABLE_NOT_EMPTY);
      }
      commitDeleteTableInfoToOMDB(tableKey);
    } catch (IOException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Delete table failed for table:{} in database:{}", tableName,
            databaseName, ex);
      }
      throw ex;
    } finally {
      metadataManager.getLock().releaseWriteLock(TABLE_LOCK, databaseName,
          tableName);
    }
  }

  private void commitDeleteTableInfoToOMDB(String dbTableKey)
      throws IOException {
    metadataManager.getMetaTable().delete(dbTableKey);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<OmTableInfo> listTables(String databaseName,
      String startTable, String tablePrefix, int maxNumOfTables)
      throws IOException {
    Preconditions.checkNotNull(databaseName);
    return metadataManager.listMetaTables(
        databaseName, startTable, tablePrefix, maxNumOfTables);
  }

  /**
   * Add acl for Ozone object. Return true if acl is added successfully else
   * false.
   *
   * @param obj Ozone object for which acl should be added.
   * @param acl ozone acl to be added.
   * @throws IOException if there is error.
   */
  @Override
  public boolean addAcl(OzoneObj obj, OzoneAcl acl) throws IOException {
    Objects.requireNonNull(obj);
    Objects.requireNonNull(acl);
    if (!obj.getResourceType().equals(OzoneObj.ResourceType.BUCKET)) {
      throw new IllegalArgumentException("Unexpected argument passed to " +
          "BucketManager. OzoneObj type:" + obj.getResourceType());
    }
    String volume = obj.getVolumeName();
    String bucket = obj.getBucketName();
    boolean changed = false;
    metadataManager.getLock().acquireWriteLock(BUCKET_LOCK, volume, bucket);
    try {
      String dbBucketKey = metadataManager.getBucketKey(volume, bucket);
      OmBucketInfo bucketInfo =
          metadataManager.getBucketTable().get(dbBucketKey);
      if (bucketInfo == null) {
        LOG.debug("Bucket:{}/{} does not exist", volume, bucket);
        throw new OMException("Bucket " + bucket + " is not found",
            BUCKET_NOT_FOUND);
      }

      changed = bucketInfo.addAcl(acl);
      if (changed) {
        metadataManager.getBucketTable().put(dbBucketKey, bucketInfo);
      }
    } catch (IOException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Add acl operation failed for bucket:{}/{} acl:{}",
            volume, bucket, acl, ex);
      }
      throw ex;
    } finally {
      metadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volume, bucket);
    }

    return changed;
  }

  /**
   * Remove acl for Ozone object. Return true if acl is removed successfully
   * else false.
   *
   * @param obj Ozone object.
   * @param acl Ozone acl to be removed.
   * @throws IOException if there is error.
   */
  @Override
  public boolean removeAcl(OzoneObj obj, OzoneAcl acl) throws IOException {
    Objects.requireNonNull(obj);
    Objects.requireNonNull(acl);
    if (!obj.getResourceType().equals(OzoneObj.ResourceType.BUCKET)) {
      throw new IllegalArgumentException("Unexpected argument passed to " +
          "BucketManager. OzoneObj type:" + obj.getResourceType());
    }
    String volume = obj.getVolumeName();
    String bucket = obj.getBucketName();
    boolean removed = false;
    metadataManager.getLock().acquireWriteLock(BUCKET_LOCK, volume, bucket);
    try {
      String dbBucketKey = metadataManager.getBucketKey(volume, bucket);
      OmBucketInfo bucketInfo =
          metadataManager.getBucketTable().get(dbBucketKey);
      if (bucketInfo == null) {
        LOG.debug("Bucket:{}/{} does not exist", volume, bucket);
        throw new OMException("Bucket " + bucket + " is not found",
            BUCKET_NOT_FOUND);
      }
      removed = bucketInfo.removeAcl(acl);
      if (removed) {
        metadataManager.getBucketTable().put(dbBucketKey, bucketInfo);
      }
    } catch (IOException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Remove acl operation failed for bucket:{}/{} acl:{}",
            volume, bucket, acl, ex);
      }
      throw ex;
    } finally {
      metadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volume, bucket);
    }
    return removed;
  }

  /**
   * Acls to be set for given Ozone object. This operations reset ACL for given
   * object to list of ACLs provided in argument.
   *
   * @param obj Ozone object.
   * @param acls List of acls.
   * @throws IOException if there is error.
   */
  @Override
  public boolean setAcl(OzoneObj obj, List<OzoneAcl> acls) throws IOException {
    Objects.requireNonNull(obj);
    Objects.requireNonNull(acls);
    if (!obj.getResourceType().equals(OzoneObj.ResourceType.BUCKET)) {
      throw new IllegalArgumentException("Unexpected argument passed to " +
          "BucketManager. OzoneObj type:" + obj.getResourceType());
    }
    String volume = obj.getVolumeName();
    String bucket = obj.getBucketName();
    metadataManager.getLock().acquireWriteLock(BUCKET_LOCK, volume, bucket);
    try {
      String dbBucketKey = metadataManager.getBucketKey(volume, bucket);
      OmBucketInfo bucketInfo =
          metadataManager.getBucketTable().get(dbBucketKey);
      if (bucketInfo == null) {
        LOG.debug("Bucket:{}/{} does not exist", volume, bucket);
        throw new OMException("Bucket " + bucket + " is not found",
            BUCKET_NOT_FOUND);
      }
      bucketInfo.setAcls(acls);
      metadataManager.getBucketTable().put(dbBucketKey, bucketInfo);
    } catch (IOException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Set acl operation failed for bucket:{}/{} acl:{}",
            volume, bucket, StringUtils.join(",", acls), ex);
      }
      throw ex;
    } finally {
      metadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volume, bucket);
    }
    return true;
  }

  /**
   * Returns list of ACLs for given Ozone object.
   *
   * @param obj Ozone object.
   * @throws IOException if there is error.
   */
  @Override
  public List<OzoneAcl> getAcl(OzoneObj obj) throws IOException {
    Objects.requireNonNull(obj);

    if (!obj.getResourceType().equals(OzoneObj.ResourceType.BUCKET)) {
      throw new IllegalArgumentException("Unexpected argument passed to " +
          "BucketManager. OzoneObj type:" + obj.getResourceType());
    }
    String volume = obj.getVolumeName();
    String bucket = obj.getBucketName();
    metadataManager.getLock().acquireReadLock(BUCKET_LOCK, volume, bucket);
    try {
      String dbBucketKey = metadataManager.getBucketKey(volume, bucket);
      OmBucketInfo bucketInfo =
          metadataManager.getBucketTable().get(dbBucketKey);
      if (bucketInfo == null) {
        LOG.debug("Bucket:{}/{} does not exist", volume, bucket);
        throw new OMException("Bucket " + bucket + " is not found",
            BUCKET_NOT_FOUND);
      }
      return bucketInfo.getAcls();
    } catch (IOException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Get acl operation failed for bucket:{}/{}.",
            volume, bucket, ex);
      }
      throw ex;
    } finally {
      metadataManager.getLock().releaseReadLock(BUCKET_LOCK, volume, bucket);
    }
  }

  /**
   * Check access for given ozoneObject.
   *
   * @param ozObject object for which access needs to be checked.
   * @param context Context object encapsulating all user related information.
   * @return true if user has access else false.
   */
  @Override
  public boolean checkAccess(OzoneObj ozObject, RequestContext context)
      throws OMException {
    Objects.requireNonNull(ozObject);
    Objects.requireNonNull(context);

    String volume = ozObject.getVolumeName();
    String bucket = ozObject.getBucketName();
    metadataManager.getLock().acquireReadLock(BUCKET_LOCK, volume, bucket);
    try {
      String dbBucketKey = metadataManager.getBucketKey(volume, bucket);
      OmBucketInfo bucketInfo =
          metadataManager.getBucketTable().get(dbBucketKey);
      if (bucketInfo == null) {
        LOG.debug("Bucket:{}/{} does not exist", volume, bucket);
        throw new OMException("Bucket " + bucket + " is not found",
            BUCKET_NOT_FOUND);
      }
      boolean hasAccess = OzoneAclUtil.checkAclRights(bucketInfo.getAcls(),
          context);
      if (LOG.isDebugEnabled()) {
        LOG.debug("user:{} has access rights for bucket:{} :{} ",
            context.getClientUgi(), ozObject.getBucketName(), hasAccess);
      }
      return hasAccess;
    } catch (IOException ex) {
      if(ex instanceof OMException) {
        throw (OMException) ex;
      }
      LOG.error("CheckAccess operation failed for bucket:{}/{}.",
          volume, bucket, ex);
      throw new OMException("Check access operation failed for " +
          "bucket:" + bucket, ex, INTERNAL_ERROR);
    } finally {
      metadataManager.getLock().releaseReadLock(BUCKET_LOCK, volume, bucket);
    }
  }
}
