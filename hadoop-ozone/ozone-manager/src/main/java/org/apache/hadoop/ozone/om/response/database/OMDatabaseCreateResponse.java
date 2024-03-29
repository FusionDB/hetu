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

package org.apache.hadoop.ozone.om.response.database;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.hm.HmDatabaseArgs;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos.PersistedUserDatabaseInfo;

import javax.annotation.Nonnull;
import java.io.IOException;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DATABASE_TABLE;

/**
 * Response for CreateDatabase request.
 */
@CleanupTableInfo(cleanupTables = DATABASE_TABLE)
public class OMDatabaseCreateResponse extends OMClientResponse {

  private PersistedUserDatabaseInfo userDatabaseInfo;
  private HmDatabaseArgs hmDatabaseArgs;

  public OMDatabaseCreateResponse(@Nonnull OMResponse omResponse,
                                  @Nonnull HmDatabaseArgs hmDatabaseArgs,
                                  @Nonnull PersistedUserDatabaseInfo userDatabaseInfo) {
    super(omResponse);
    this.hmDatabaseArgs = hmDatabaseArgs;
    this.userDatabaseInfo = userDatabaseInfo;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public OMDatabaseCreateResponse(@Nonnull OMResponse omResponse) {
    super(omResponse);
    checkStatusNotOK();
  }

  @Override
  protected void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    String dbDatabaseKey =
        omMetadataManager.getDatabaseKey(hmDatabaseArgs.getName());
    String dbUserKey =
        omMetadataManager.getUserKey(hmDatabaseArgs.getOwnerName());

    omMetadataManager.getDatabaseTable().putWithBatch(batchOperation,
        dbDatabaseKey, hmDatabaseArgs);
    omMetadataManager.getUserTableDb().putWithBatch(batchOperation, dbUserKey,
        userDatabaseInfo);
  }

  @VisibleForTesting
  public HmDatabaseArgs getOmDatabaseArgs() {
    return hmDatabaseArgs;
  }

}

