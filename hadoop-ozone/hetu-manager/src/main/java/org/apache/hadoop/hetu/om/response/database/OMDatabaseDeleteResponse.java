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

package org.apache.hadoop.hetu.om.response.database;

import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.hetu.om.response.CleanupTableInfo;
import org.apache.hadoop.hetu.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos.PersistedUserDatabaseInfo;

import javax.annotation.Nonnull;
import java.io.IOException;

import static org.apache.hadoop.hetu.om.OmMetadataManagerImpl.DATABASE_TABLE;
import static org.apache.hadoop.hetu.om.OmMetadataManagerImpl.VOLUME_TABLE;

/**
 * Response for DeleteDatabase request.
 */
@CleanupTableInfo(cleanupTables = {DATABASE_TABLE})
public class OMDatabaseDeleteResponse extends OMClientResponse {
  private String database;
  private String owner;
  private PersistedUserDatabaseInfo updatedDatabaseList;

  public OMDatabaseDeleteResponse(@Nonnull OMResponse omResponse,
                                  @Nonnull String database, @Nonnull String owner,
                                  @Nonnull PersistedUserDatabaseInfo updatedDatabaseList) {
    super(omResponse);
    this.database = database;
    this.owner = owner;
    this.updatedDatabaseList = updatedDatabaseList;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public OMDatabaseDeleteResponse(@Nonnull OMResponse omResponse) {
    super(omResponse);
    checkStatusNotOK();
  }

  @Override
  protected void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    String dbUserKey = omMetadataManager.getUserKey(owner);
    PersistedUserDatabaseInfo databaseList = updatedDatabaseList;
    if (updatedDatabaseList.getDatabaseNamesList().size() == 0) {
      omMetadataManager.getUserTableDb().deleteWithBatch(batchOperation,
          dbUserKey);
    } else {
      omMetadataManager.getUserTableDb().putWithBatch(batchOperation, dbUserKey,
              databaseList);
    }
    omMetadataManager.getDatabaseTable().deleteWithBatch(batchOperation,
        omMetadataManager.getDatabaseKey(database));
  }
}

