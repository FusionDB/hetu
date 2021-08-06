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

package org.apache.hadoop.hetu.om.response.partition;

import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.hm.OmDatabaseArgs;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmPartitionInfo;
import org.apache.hadoop.ozone.om.helpers.OmTableInfo;
import org.apache.hadoop.hetu.om.response.CleanupTableInfo;
import org.apache.hadoop.hetu.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;

import static org.apache.hadoop.hetu.om.OmMetadataManagerImpl.PARTITION_TABLE;

/**
 * Response for CreatePartition request.
 */
@CleanupTableInfo(cleanupTables = {PARTITION_TABLE})
public final class OMPartitionCreateResponse extends OMClientResponse {

  private final OmPartitionInfo omPartitionInfo;
  private final OmTableInfo omTableInfo;
  private final OmDatabaseArgs omDatabaseArgs;

  public OMPartitionCreateResponse(@Nonnull OMResponse omResponse,
                                   @Nonnull OmPartitionInfo omPartitionInfo,
                                   @Nonnull OmTableInfo omTableInfo,
                                   @Nonnull OmDatabaseArgs omDatabaseArgs) {
    super(omResponse);
    this.omPartitionInfo = omPartitionInfo;
    this.omTableInfo = omTableInfo;
    this.omDatabaseArgs = omDatabaseArgs;
  }

  public OMPartitionCreateResponse(@Nonnull OMResponse omResponse,
                                   @Nonnull OmPartitionInfo omPartitionInfo,
                                   @Nonnull OmTableInfo omTableInfo) {
    super(omResponse);
    this.omPartitionInfo = omPartitionInfo;
    this.omTableInfo = omTableInfo;
    this.omDatabaseArgs = null;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public OMPartitionCreateResponse(@Nonnull OMResponse omResponse) {
    super(omResponse);
    checkStatusNotOK();
    omPartitionInfo = null;
    omTableInfo = null;
    omDatabaseArgs = null;
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    String dbPartitionKey = omMetadataManager.getPartitionKey(omPartitionInfo.getDatabaseName(),
            omPartitionInfo.getTableName(), omPartitionInfo.getPartitionName());
    omMetadataManager.getPartitionTable().putWithBatch(batchOperation,
           dbPartitionKey, omPartitionInfo);

    // update table usedCapacityInBytes
    if (omTableInfo != null) {
      omMetadataManager.getMetaTable().putWithBatch(batchOperation,
              omMetadataManager.getMetaTableKey(omTableInfo.getDatabaseName(), omTableInfo.getTableName()),
              omTableInfo);
    }
    // update database usedCapacityInBytes
    if (omDatabaseArgs != null) {
      omMetadataManager.getDatabaseTable().putWithBatch(batchOperation,
              omMetadataManager.getDatabaseKey(omDatabaseArgs.getName()),
              omDatabaseArgs);
    }
  }

  @Nullable
  public OmTableInfo getOmTableInfo() {
    return omTableInfo;
  }

  @Nullable
  public OmPartitionInfo getOmPartitionInfo() {
    return omPartitionInfo;
  }

}

