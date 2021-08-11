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

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hetu.hm.helpers.OmDatabaseArgs;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos.PersistedUserDatabaseInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_USER_MAX_DATABASE;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_USER_MAX_DATABASE_DEFAULT;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.DATABASE_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.USER_LOCK;

/**
 * OM database management code.
 */
public class DatabaseManagerImpl implements DatabaseManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(DatabaseManagerImpl.class);

  private final OMMetadataManager metadataManager;
  private final int maxUserDatabaseCount;

  /**
   * Constructor.
   * @param conf - Ozone configuration.
   * @throws IOException
   */
  public DatabaseManagerImpl(OMMetadataManager metadataManager,
                             OzoneConfiguration conf) {
    this.metadataManager = metadataManager;
    this.maxUserDatabaseCount = conf.getInt(OZONE_OM_USER_MAX_DATABASE,
        OZONE_OM_USER_MAX_DATABASE_DEFAULT);
  }

  // Helpers to add and delete database from user list
  private PersistedUserDatabaseInfo addDatabaseToOwnerList(
      String database, String owner) throws IOException {
    // Get the database list
    String dbUserKey = metadataManager.getUserKey(owner);
    PersistedUserDatabaseInfo databaseList =
        metadataManager.getUserTableDb().get(dbUserKey);
    List<String> prevDbList = new ArrayList<>();
    if (databaseList != null) {
      prevDbList.addAll(databaseList.getDatabaseNamesList());
    }

    // Check the database count
    if (prevDbList.size() >= maxUserDatabaseCount) {
      LOG.debug("Too many databases for user:{}", owner);
      throw new OMException("Too many databases for user:" + owner,
          ResultCodes.USER_TOO_MANY_DATABASES);
    }

    // Add the new database to the list
    prevDbList.add(database);
    PersistedUserDatabaseInfo newDbList = PersistedUserDatabaseInfo.newBuilder()
        .addAllDatabaseNames(prevDbList).build();

    return newDbList;
  }

  /**
   * Creates a database.
   * @param omDatabaseArgs - HmDatabaseArgs.
   */
  @Override
  public void createDatabase(OmDatabaseArgs omDatabaseArgs) throws IOException {
    Preconditions.checkNotNull(omDatabaseArgs);

    boolean acquiredUserLock = false;
    metadataManager.getLock().acquireWriteLock(DATABASE_LOCK,
            omDatabaseArgs.getName());
    try {
      acquiredUserLock = metadataManager.getLock().acquireWriteLock(USER_LOCK,
          omDatabaseArgs.getOwnerName());
      String databaseKey = metadataManager.getDatabaseKey(
          omDatabaseArgs.getName());
      String dbUserKey = metadataManager.getUserKey(
          omDatabaseArgs.getOwnerName());
      OmDatabaseArgs databaseInfo =
          metadataManager.getDatabaseTable().get(databaseKey);

      // Check of the database already exists
      if (databaseInfo != null) {
        LOG.debug("database:{} already exists", omDatabaseArgs.getName());
        throw new OMException(ResultCodes.DATABASE_ALREADY_EXISTS);
      }

      PersistedUserDatabaseInfo databsaeList = addDatabaseToOwnerList(
              omDatabaseArgs.getName(), omDatabaseArgs.getOwnerName());

      // Set creation time
      omDatabaseArgs.setCreationTime(System.currentTimeMillis());

      createDatabaseCommitToDB(omDatabaseArgs, databsaeList, databaseKey,
            dbUserKey);

      LOG.debug("created database:{} user:{}", omDatabaseArgs.getName(),
              omDatabaseArgs.getOwnerName());
    } catch (IOException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Database creation failed for user:{} database:{}",
                omDatabaseArgs.getOwnerName(), omDatabaseArgs.getName(), ex);
      }
      throw ex;
    } finally {
      if (acquiredUserLock) {
        metadataManager.getLock().releaseWriteLock(USER_LOCK,
            omDatabaseArgs.getOwnerName());
      }
      metadataManager.getLock().releaseWriteLock(DATABASE_LOCK,
              omDatabaseArgs.getName());
    }
  }

  private void createDatabaseCommitToDB(OmDatabaseArgs omDatabaseArgs,
                                        PersistedUserDatabaseInfo databaseList, String dbDatabaseKey, String dbUserKey)
      throws IOException {
    try (BatchOperation batch = metadataManager.getStore()
        .initBatchOperation()) {
      // Write the db info
      metadataManager.getDatabaseTable().putWithBatch(batch, dbDatabaseKey,
              omDatabaseArgs);
      metadataManager.getUserTableDb().putWithBatch(batch, dbUserKey,
              databaseList);
      // Add db to user list
      metadataManager.getStore().commitBatchOperation(batch);
    } catch (IOException ex) {
      throw ex;
    }
  }

  /**
   * Changes the owner of a database.
   *
   * @param database - Name of the database.
   * @param owner - Name of the owner.
   * @throws IOException
   */
  @Override
  public void setOwner(String database, String owner)
      throws IOException {
    Preconditions.checkNotNull(database);
    Preconditions.checkNotNull(owner);
    boolean acquiredUsersLock = false;
    String actualOwner = null;
    metadataManager.getLock().acquireWriteLock(DATABASE_LOCK, database);
    try {
      String dbDatabaseKey = metadataManager.getDatabaseKey(database);
      OmDatabaseArgs databaseArgs = metadataManager
          .getDatabaseTable().get(dbDatabaseKey);
      if (databaseArgs == null) {
        LOG.debug("Changing database ownership failed for user:{} database:{}",
            owner, database);
        throw new OMException("Database " + database + " is not found",
            ResultCodes.DATABASE_NOT_FOUND);
      }

      Preconditions.checkState(database.equals(databaseArgs.getName()));

      actualOwner = databaseArgs.getOwnerName();
      String originalOwner = metadataManager.getUserKey(actualOwner);

      acquiredUsersLock = metadataManager.getLock().acquireMultiUserLock(owner,
          originalOwner);
      PersistedUserDatabaseInfo oldOwnerDatabaseList =
          delDatabaseFromOwnerList(database, originalOwner);

      String newOwner =  metadataManager.getUserKey(owner);
      PersistedUserDatabaseInfo newOwnerDatabaseList = addDatabaseToOwnerList(database,
          newOwner);

      databaseArgs.setOwnerName(owner);
      setOwnerCommitToDB(oldOwnerDatabaseList, newOwnerDatabaseList,
              databaseArgs, owner);
    } catch (IOException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Changing database ownership failed for user:{} database:{}",
            owner, database, ex);
      }
      throw ex;
    } finally {
      if (acquiredUsersLock) {
        metadataManager.getLock().releaseMultiUserLock(owner, actualOwner);
      }
      metadataManager.getLock().releaseWriteLock(DATABASE_LOCK, database);
    }
  }

  private void setOwnerCommitToDB(PersistedUserDatabaseInfo oldOwnerDatabaseList,
                                  PersistedUserDatabaseInfo newOwnerDatabaseList,
                                  OmDatabaseArgs newOwnerDatabaseArgs, String oldOwner) throws IOException {
    try (BatchOperation batch = metadataManager.getStore()
        .initBatchOperation()) {
      if (oldOwnerDatabaseList.getDatabaseNamesList().size() == 0) {
        metadataManager.getUserTableDb().deleteWithBatch(batch, oldOwner);
      } else {
        metadataManager.getUserTableDb().putWithBatch(batch, oldOwner,
                oldOwnerDatabaseList);
      }
      metadataManager.getUserTableDb().putWithBatch(batch,
              newOwnerDatabaseArgs.getOwnerName(),
              newOwnerDatabaseList);

      String dbDatabaseKey =
          metadataManager.getDatabaseKey(newOwnerDatabaseArgs.getName());
      metadataManager.getDatabaseTable().putWithBatch(batch,
              dbDatabaseKey, newOwnerDatabaseArgs);
      metadataManager.getStore().commitBatchOperation(batch);
    }
  }

  /**
   * Gets the database information.
   * @param database - database name.
   * @return DatabaseArgs or exception is thrown.
   * @throws IOException
   */
  @Override
  public OmDatabaseArgs getDatabaseInfo(String database) throws IOException {
    Preconditions.checkNotNull(database);
    metadataManager.getLock().acquireReadLock(DATABASE_LOCK, database);
    try {
      String dbDatabaseKey = metadataManager.getDatabaseKey(database);
      OmDatabaseArgs omDatabaseArgs =
          metadataManager.getDatabaseTable().get(dbDatabaseKey);
      if (omDatabaseArgs == null) {
        LOG.debug("database:{} does not exist", database);
        throw new OMException("Database " + database + " is not found",
            ResultCodes.DATABASE_NOT_FOUND);
      }

      return omDatabaseArgs;
    } catch (IOException ex) {
      if (!(ex instanceof OMException)) {
        LOG.warn("Info database failed for database:{}", database, ex);
      }
      throw ex;
    } finally {
      metadataManager.getLock().releaseReadLock(DATABASE_LOCK, database);
    }
  }

  /**
   * Deletes an existing empty database.
   *
   * @param database - Name of the database.
   * @throws IOException
   */
  @Override
  public void deleteDatabase(String database) throws IOException {
    Preconditions.checkNotNull(database);
    String owner = null;
    boolean acquiredUserLock = false;
    metadataManager.getLock().acquireWriteLock(DATABASE_LOCK, database);
    try {
      owner = getDatabaseInfo(database).getOwnerName();
      acquiredUserLock = metadataManager.getLock().acquireWriteLock(USER_LOCK,
          owner);
      String dbDatabaseKey = metadataManager.getDatabaseKey(database);
      OmDatabaseArgs databaseArgs =
          metadataManager.getDatabaseTable().get(dbDatabaseKey);
      if (databaseArgs == null) {
        LOG.debug("database:{} does not exist", database);
        throw new OMException("Database " + database + " is not found",
            ResultCodes.DATABASE_NOT_FOUND);
      }

      if (!metadataManager.isDatabaseEmpty(database)) {
        LOG.debug("database:{} is not empty", database);
        throw new OMException(ResultCodes.DATABASE_NOT_EMPTY);
      }
      Preconditions.checkState(database.equals(databaseArgs.getName()));
      // delete the database from the owner list
      // as well as delete the database entry
      PersistedUserDatabaseInfo newDatabaseList = delDatabaseFromOwnerList(database,
              databaseArgs.getOwnerName());


      deleteDatabaseCommitToDB(newDatabaseList, database, owner);
    } catch (IOException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Delete database failed for database:{}", database, ex);
      }
      throw ex;
    } finally {
      if (acquiredUserLock) {
        metadataManager.getLock().releaseWriteLock(USER_LOCK, owner);
      }
      metadataManager.getLock().releaseWriteLock(DATABASE_LOCK, database);

    }
  }

  private PersistedUserDatabaseInfo delDatabaseFromOwnerList(
          String database, String owner) throws IOException {
    // Get the database list
    PersistedUserDatabaseInfo databaseList =
            metadataManager.getUserTableDb().get(owner);
    List<String> prevDbList = new ArrayList<>();
    if (databaseList != null) {
      prevDbList.addAll(databaseList.getDatabaseNamesList());
    } else {
      LOG.debug("database:{} not found for user:{}", database, owner);
      throw new OMException(ResultCodes.USER_NOT_FOUND);
    }

    // Remove the database from the list
    prevDbList.remove(database);
    PersistedUserDatabaseInfo newDbList = PersistedUserDatabaseInfo.newBuilder()
            .addAllDatabaseNames(prevDbList).build();
    return newDbList;
  }

  private void deleteDatabaseCommitToDB(PersistedUserDatabaseInfo newDatabaseList,
      String database, String owner) throws IOException {
    try (BatchOperation batch = metadataManager.getStore()
        .initBatchOperation()) {
      String dbUserKey = metadataManager.getUserKey(owner);
      if (newDatabaseList.getDatabaseNamesList().size() == 0) {
        metadataManager.getUserTableDb().deleteWithBatch(batch, dbUserKey);
      } else {
        metadataManager.getUserTableDb().putWithBatch(batch, dbUserKey,
                newDatabaseList);
      }
      metadataManager.getDatabaseTable().deleteWithBatch(batch,
          metadataManager.getDatabaseKey(database));
      metadataManager.getStore().commitBatchOperation(batch);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<OmDatabaseArgs> listDatabase(String userName,
                                           String prefix, String startKey, int maxKeys) throws IOException {
    metadataManager.getLock().acquireReadLock(USER_LOCK, userName);
    try {
      return metadataManager.listDatabase(userName, prefix, startKey, maxKeys);
    } finally {
      metadataManager.getLock().releaseReadLock(USER_LOCK, userName);
    }
  }
}
