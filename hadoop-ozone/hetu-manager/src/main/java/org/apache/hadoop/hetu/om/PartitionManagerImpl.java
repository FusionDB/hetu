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
import org.apache.hadoop.ozone.om.helpers.OmPartitionArgs;
import org.apache.hadoop.ozone.om.helpers.OmPartitionInfo;
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
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.PARTITION_NOT_FOUND;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.PARTITION_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.TABLE_LOCK;

/**
 * OM partition manager.
 */
public class PartitionManagerImpl implements PartitionManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(PartitionManagerImpl.class);

  /**
   * OMMetadataManager is used for accessing OM MetadataDB and ReadWriteLock.
   */
  private final OMMetadataManager metadataManager;
  private final KeyProviderCryptoExtension kmsProvider;

  /**
   * Constructs partitionManager.
   *
   * @param metadataManager
   */
  public PartitionManagerImpl(OMMetadataManager metadataManager) {
    this(metadataManager, null, false);
  }

  public PartitionManagerImpl(OMMetadataManager metadataManager,
                              KeyProviderCryptoExtension kmsProvider) {
    this(metadataManager, kmsProvider, false);
  }

  public PartitionManagerImpl(OMMetadataManager metadataManager,
                              KeyProviderCryptoExtension kmsProvider, boolean isRatisEnabled) {
    this.metadataManager = metadataManager;
    this.kmsProvider = kmsProvider;
  }

  KeyProviderCryptoExtension getKMSProvider() {
    return kmsProvider;
  }

  /**
   * MetadataDB is maintained in MetadataManager and shared between
   * PartitionManager, TableManager And DatabaseManager. (and also by BlockManager)
   *
   * PartitionManager uses MetadataDB to store partition level information.
   *
   * Keys used in PartitionManager for storing data into MetadataDB
   * for PartitionInfo:
   * {database/table/partition} -> partitionInfo
   *
   * Work flow of create partition:
   *
   * -> Check if the Database exists in metadataDB, if not throw
   * DatabaseNotFoundException.
   * -> Else check if the Partition exists in metadataDB, if so throw
   * PartitionExistException
   * -> Else update MetadataDB with TableInfo.
   */

  /**
   * Creates a partition.
   *
   * @param partitionInfo - OmPartitionInfo.
   */
  @Override
  public void createPartition(OmPartitionInfo partitionInfo) throws IOException {
    Preconditions.checkNotNull(partitionInfo);
    String databaseName = partitionInfo.getDatabaseName();
    String tableName = partitionInfo.getTableName();
    String partitionName = partitionInfo.getPartitionName();
    boolean acquiredPartitionLock = false;
    metadataManager.getLock().acquireWriteLock(TABLE_LOCK, databaseName, tableName);
    try {
      acquiredPartitionLock = metadataManager.getLock().acquireWriteLock(
          PARTITION_LOCK, databaseName, tableName, partitionName);
      String databaseKey = metadataManager.getDatabaseKey(databaseName);
      String tableKey = metadataManager.getMetaTableKey(databaseName, tableName);
      String partitionKey = metadataManager.getPartitionKey(databaseName, tableName, partitionName);
      OmDatabaseArgs databaseArgs = metadataManager.getDatabaseTable().get(databaseKey);

      //Check if the database exists
      if (databaseArgs == null) {
        LOG.debug("database: {} not found ", databaseName);
        throw new OMException("Database doesn't exist",
            OMException.ResultCodes.DATABASE_NOT_FOUND);
      }

      //Check if the table exists
      OmTableInfo omTableInfo = metadataManager.getMetaTable().get(tableKey);
      if (omTableInfo == null) {
        LOG.debug("table: {} not found in database: {}", tableName, databaseName);
        throw new OMException("Table doesn't exist",
                OMException.ResultCodes.TABLE_NOT_FOUND);
      }

      //Check if partition already exists
      if (metadataManager.getPartitionTable().get(partitionKey) != null) {
        LOG.debug("partition: {} already exists ", partitionName);
        throw new OMException("Partition already exist",
            OMException.ResultCodes.PARTITION_ALREADY_EXISTS);
      }

      OmPartitionInfo.Builder omPartitionInfoBuilder = partitionInfo.toBuilder()
          .setCreationTime(Time.now());

      // TODO acl
//      OzoneAclUtil.inheritDefaultAcls(omPartitionInfoBuilder.getAcls(),
//          databaseArgs.getDefaultAcls());

      OmPartitionInfo omPartitionInfo = omPartitionInfoBuilder.build();
      commitPartitionInfoToDB(omPartitionInfo);

      LOG.debug("created partition: {} in database: {} in table: {}", partitionName,
            databaseName, tableName);
    } catch (IOException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Partition creation failed for partition:{} in database: {} in table: {}",
            partitionName, databaseName, tableName, ex);
      }
      throw ex;
    } finally {
      if (acquiredPartitionLock) {
        metadataManager.getLock().releaseWriteLock(PARTITION_LOCK, databaseName,
            tableName, partitionName);
      }
      metadataManager.getLock().releaseWriteLock(TABLE_LOCK, databaseName, tableName);
    }
  }

  private void commitPartitionInfoToDB(OmPartitionInfo omPartitoinInfo)
          throws IOException {
    String dbPartitionKey =
            metadataManager.getPartitionKey(omPartitoinInfo.getDatabaseName(),
                    omPartitoinInfo.getTableName(), omPartitoinInfo.getPartitionName());
    metadataManager.getPartitionTable().put(dbPartitionKey,
            omPartitoinInfo);
  }

  /**
   * Returns Partition Information.
   *
   * @param databaseName - Name of the Database.
   * @param tableName - Name of the Table.
   * @param partitionName - Name of the Partition.
   */
  @Override
  public OmPartitionInfo getPartitionInfo(String databaseName, String tableName, String partitionName)
      throws IOException {
    Preconditions.checkNotNull(databaseName);
    Preconditions.checkNotNull(tableName);
    Preconditions.checkNotNull(partitionName);
    metadataManager.getLock().acquireReadLock(PARTITION_LOCK, databaseName,
        tableName, partitionName);
    try {
      String partitionKey = metadataManager.getPartitionKey(databaseName, tableName, partitionName);
      OmPartitionInfo value = metadataManager.getPartitionTable().get(partitionKey);
      if (value == null) {
        LOG.debug("partition: {} not found in database: {} in table: {}.", partitionName,
            databaseName, tableName);
        throw new OMException("Partition not found",
            PARTITION_NOT_FOUND);
      }
      return value;
    } catch (IOException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Exception while getting partition info for table: {}",
            partitionName, ex);
      }
      throw ex;
    } finally {
      metadataManager.getLock().releaseReadLock(PARTITION_LOCK, databaseName,
          tableName, partitionName);
    }
  }

  /**
   * Sets partition property from args.
   *
   * @param args - PartitionArgs.
   * @throws IOException - On Failure.
   */
  @Override
  public void setPartitionProperty(OmPartitionArgs args) throws IOException {
    Preconditions.checkNotNull(args);
    String databaseName = args.getDatabaseName();
    String tableName = args.getTableName();
    String partitionName = args.getPartitionName();
    metadataManager.getLock().acquireWriteLock(PARTITION_LOCK, databaseName,
        tableName, partitionName);
    try {
      String partitionKey = metadataManager.getPartitionKey(databaseName, tableName, partitionName);
      OmPartitionInfo oldPartitionInfo =
          metadataManager.getPartitionTable().get(partitionKey);
      //Check if partition exist
      if (oldPartitionInfo == null) {
        LOG.debug("partition: {} not found ", partitionName);
        throw new OMException("Partition doesn't exist",
            PARTITION_NOT_FOUND);
      }
      OmPartitionInfo.Builder partitionInfoBuilder = OmPartitionInfo.newBuilder();
      partitionInfoBuilder.setDatabaseName(oldPartitionInfo.getDatabaseName());
      partitionInfoBuilder.setTableName(oldPartitionInfo.getTableName());
      partitionInfoBuilder.setPartitionName(partitionName);
      partitionInfoBuilder.addAllMetadata(args.getMetadata());

      //Check StorageType to update
      StorageType storageType = args.getStorageType();
      if (storageType != null) {
        partitionInfoBuilder.setStorageType(storageType);
        LOG.debug("Updating partition storage type for partition: {} in database: {} of table: {}",
            partitionName, databaseName, tableName);
      } else {
        partitionInfoBuilder.setStorageType(oldPartitionInfo.getStorageType());
      }

      //Check Versioning to update
      Boolean versioning = args.getIsVersionEnabled();
      if (versioning != null) {
        partitionInfoBuilder.setIsVersionEnabled(versioning);
        LOG.debug("Updating partition versioning for partition: {} in database: {} of table: {}",
            partitionName, databaseName, tableName);
      } else {
        partitionInfoBuilder
            .setIsVersionEnabled(oldPartitionInfo.getIsVersionEnabled());
      }
      partitionInfoBuilder.setCreationTime(oldPartitionInfo.getCreationTime());

      // Set acls from oldPartitionInfo if it has any.
//      if (oldPartitionInfo.getAcls() != null) {
//        partitionInfoBuilder.setAcls(oldPartitionInfo.getAcls());
//      }

      OmPartitionInfo omPartitionInfo = partitionInfoBuilder.build();

      commitPartitionInfoToDB(omPartitionInfo);
    } catch (IOException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Setting partition property failed for partition:{} in database:{} of table: {}",
            partitionName, databaseName, tableName, ex);
      }
      throw ex;
    } finally {
      metadataManager.getLock().releaseWriteLock(PARTITION_LOCK, databaseName,
          tableName, partitionName);
    }
  }

  /**
   * Deletes an existing empty partition from table.
   *
   * @param databaseName - Name of the Database.
   * @param tableName - Name of the Table.
   * @param partitionName - Name of the Partition.
   * @throws IOException - on Failure.
   */
  @Override
  public void deletePartition(String databaseName, String tableName, String partitionName)
      throws IOException {
    Preconditions.checkNotNull(databaseName);
    Preconditions.checkNotNull(tableName);
    Preconditions.checkNotNull(partitionName);
    metadataManager.getLock().acquireWriteLock(PARTITION_LOCK, databaseName,
        tableName, partitionName);
    try {
      //Check if partition exists
      String partitionKey = metadataManager.getPartitionKey(databaseName, tableName, partitionName);
      if (metadataManager.getPartitionTable().get(partitionKey) == null) {
        LOG.debug("partition: {} not found ", partitionName);
        throw new OMException("Partition doesn't exist",
            PARTITION_NOT_FOUND);
      }
      //Check if partition is empty
      if (!metadataManager.isPartitionEmpty(databaseName, tableName, partitionName)) {
        LOG.debug("partition: {} is not empty ", partitionName);
        throw new OMException("Partition is not empty",
            OMException.ResultCodes.PARTITION_NOT_EMPTY);
      }
      commitDeletePartitionInfoToOMDB(partitionKey);
    } catch (IOException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Delete partition failed for table:{} in database:{} of table: {}", partitionName,
            databaseName, tableName, ex);
      }
      throw ex;
    } finally {
      metadataManager.getLock().releaseWriteLock(PARTITION_LOCK, databaseName,
          tableName, partitionName);
    }
  }

  private void commitDeletePartitionInfoToOMDB(String dbPartitionKey)
      throws IOException {
    metadataManager.getPartitionTable().delete(dbPartitionKey);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<OmPartitionInfo> listPartitions(String databaseName, String tableName,
      String startPartition, String partitionPrefix, int maxNumOfPartitions)
      throws IOException {
    Preconditions.checkNotNull(databaseName);
    Preconditions.checkNotNull(tableName);
    return metadataManager.listPartitions(
        databaseName, tableName, startPartition, partitionPrefix, maxNumOfPartitions);
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
