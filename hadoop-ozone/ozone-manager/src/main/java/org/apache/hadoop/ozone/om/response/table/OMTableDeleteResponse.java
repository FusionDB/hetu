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

package org.apache.hadoop.ozone.om.response.table;

import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hetu.hm.helpers.OmDatabaseArgs;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

import javax.annotation.Nonnull;
import java.io.IOException;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.META_TABLE;

/**
 * Response for DeleteTable request.
 */
@CleanupTableInfo(cleanupTables = {META_TABLE})
public final class OMTableDeleteResponse extends OMClientResponse {

  private String databaseName;
  private String tableName;
  private final OmDatabaseArgs omDatabaseArgs;

  public OMTableDeleteResponse(@Nonnull OMResponse omResponse,
                               String databaseName, String tableName, OmDatabaseArgs omDatabaseArgs) {
    super(omResponse);
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.omDatabaseArgs = omDatabaseArgs;
  }

  public OMTableDeleteResponse(@Nonnull OMResponse omResponse,
                               String databaseName, String tableName) {
    super(omResponse);
    this.databaseName = tableName;
    this.tableName = tableName;
    this.omDatabaseArgs = null;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public OMTableDeleteResponse(@Nonnull OMResponse omResponse) {
    super(omResponse);
    checkStatusNotOK();
    this.omDatabaseArgs = null;
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    String dbTableKey =
        omMetadataManager.getMetaTableKey(databaseName, tableName);
    omMetadataManager.getMetaTable().deleteWithBatch(batchOperation,
           dbTableKey);
    // update database info
    if (omDatabaseArgs != null) {
      omMetadataManager.getDatabaseTable().putWithBatch(batchOperation,
              omMetadataManager.getDatabaseKey(omDatabaseArgs.getName()),
              omDatabaseArgs);
    }
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public String getTableName() {
    return tableName;
  }

}

