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

package org.apache.hadoop.hetu.om.request.database;

import com.google.common.base.Optional;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.hetu.hm.helpers.OmDatabaseArgs;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.hetu.om.request.OMClientRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos.PersistedUserDatabaseInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Defines common methods required for database requests.
 */
public abstract class OMDatabaseRequest extends OMClientRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMDatabaseRequest.class);

  public OMDatabaseRequest(OMRequest omRequest) {
    super(omRequest);
  }

  /**
   * Delete database from user database list. This method should be called after
   * acquiring user lock.
   * @param databaseList - current database list owned by user.
   * @param database - database which needs to deleted from the database list.
   * @param owner - Name of the Owner.
   * @param txID - The transaction ID that is updating this value.
   * @return UserDatabaseInfo - updated UserDatabaseInfo.
   * @throws IOException
   */
  protected PersistedUserDatabaseInfo delDatabaseFromOwnerList(
          PersistedUserDatabaseInfo databaseList, String database,
          String owner, long txID) throws IOException {

    List<String> prevDbList = new ArrayList<>();

    if (databaseList != null) {
      prevDbList.addAll(databaseList.getDatabaseNamesList());
    } else {
      // No Database for this user
      throw new OMException("User not found: " + owner,
          OMException.ResultCodes.USER_NOT_FOUND);
    }

    // Remove the database from the list
    prevDbList.remove(database);
    PersistedUserDatabaseInfo newDbList = PersistedUserDatabaseInfo.newBuilder()
        .addAllDatabaseNames(prevDbList)
            .setObjectID(databaseList.getObjectID())
            .setUpdateID(txID)
         .build();
    return newDbList;
  }


  /**
   * Add database to user database list. This method should be called after
   * acquiring user lock.
   * @param databaseList - current database list owned by user.
   * @param database - database which needs to be added to this list.
   * @param owner
   * @param maxUserDatabaseCount
   * @return databaseList - which is updated database list.
   * @throws OMException - if user has databases greater than
   * maxUserDatabaseCount, an exception is thrown.
   */
  protected PersistedUserDatabaseInfo addDatabaseToOwnerList(
          PersistedUserDatabaseInfo databaseList, String database, String owner,
      long maxUserDatabaseCount, long txID) throws IOException {

    // Check the database count
    if (databaseList != null &&
            databaseList.getDatabaseNamesList().size() >= maxUserDatabaseCount) {
      throw new OMException("Too many databases for user:" + owner,
          OMException.ResultCodes.USER_TOO_MANY_DATABASES);
    }

    Set<String> databaseSet = new HashSet<>();
    long objectID = txID;
    if (databaseList != null) {
      databaseSet.addAll(databaseList.getDatabaseNamesList());
      objectID = databaseList.getObjectID();
    }

    databaseSet.add(database);
    return PersistedUserDatabaseInfo.newBuilder()
        .setObjectID(objectID)
        .setUpdateID(txID)
        .addAllDatabaseNames(databaseSet).build();
  }

  /**
   * Create Hetu Database. This method should be called after acquiring user
   * and database Lock.
   * @param omMetadataManager
   * @param omDatabaseArgs
   * @param databaseList
   * @param dbDatabaseKey
   * @param dbUserKey
   * @param transactionLogIndex
   * @throws IOException
   */
  protected void createDatabase(
      final OMMetadataManager omMetadataManager, OmDatabaseArgs omDatabaseArgs,
      PersistedUserDatabaseInfo databaseList, String dbDatabaseKey,
      String dbUserKey, long transactionLogIndex) {
    // Update cache: Update user and database cache.
    omMetadataManager.getUserTableDb().addCacheEntry(new CacheKey<>(dbUserKey),
        new CacheValue<>(Optional.of(databaseList), transactionLogIndex));

    omMetadataManager.getDatabaseTable().addCacheEntry(
        new CacheKey<>(dbDatabaseKey),
        new CacheValue<>(Optional.of(omDatabaseArgs), transactionLogIndex));
  }

  /**
   * Return database info for the specified database. This method should be
   * called after acquiring database lock.
   * @param omMetadataManager
   * @param database
   * @return HmDatabaseArgs
   * @throws IOException
   */
  protected OmDatabaseArgs getDatabaseInfo(OMMetadataManager omMetadataManager,
                                           String database) throws IOException {

    String dbDatabaseKey = omMetadataManager.getDatabaseKey(database);
    OmDatabaseArgs omDatabaseArgs =
        omMetadataManager.getDatabaseTable().get(dbDatabaseKey);
    if (omDatabaseArgs == null) {
      throw new OMException("Database " + database + " is not found",
          OMException.ResultCodes.DATABASE_NOT_FOUND);
    }
    return omDatabaseArgs;
  }
}
