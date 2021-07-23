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

package org.apache.hadoop.ozone.om.response.partition;

import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmTableInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

import javax.annotation.Nonnull;
import java.io.IOException;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.PARTITION_TABLE;

/**
 * Response for DeletePartition request.
 */
@CleanupTableInfo(cleanupTables = {PARTITION_TABLE})
public final class OMPartitionDeleteResponse extends OMClientResponse {

  private String databaseName;
  private String tableName;
  private String partitionName;
  private final OmTableInfo omTableInfo;

  public OMPartitionDeleteResponse(@Nonnull OMResponse omResponse,
                                   String databaseName, String tableName,
                                   String partitionName, OmTableInfo omTableInfo) {
    super(omResponse);
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.partitionName = partitionName;
    this.omTableInfo = omTableInfo;
  }

  public OMPartitionDeleteResponse(@Nonnull OMResponse omResponse,
                                   String databaseName, String tableName,
                                   String partitionName) {
    super(omResponse);
    this.databaseName = tableName;
    this.tableName = tableName;
    this.partitionName = partitionName;
    this.omTableInfo = null;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public OMPartitionDeleteResponse(@Nonnull OMResponse omResponse) {
    super(omResponse);
    checkStatusNotOK();
    this.omTableInfo = null;
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    String dbPartitionKey =
        omMetadataManager.getPartitionKey(databaseName, tableName, partitionName);
    omMetadataManager.getPartitionTable().deleteWithBatch(batchOperation,
           dbPartitionKey);
    // update table info
    if (omTableInfo != null) {
      omMetadataManager.getMetaTable().putWithBatch(batchOperation,
              omMetadataManager.getMetaTableKey(omTableInfo.getDatabaseName(), omTableInfo.getTableName()),
              omTableInfo);
    }
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public String getTableName() {
    return tableName;
  }

  public String getPartitionName() {
    return partitionName;
  }

}

