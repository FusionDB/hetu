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
import org.apache.hadoop.ozone.hm.HmDatabaseArgs;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmTableInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.META_TABLE;

/**
 * Response for CreateTable request.
 */
@CleanupTableInfo(cleanupTables = {META_TABLE})
public final class OMTableCreateResponse extends OMClientResponse {

  private final OmTableInfo omTableInfo;
  private final HmDatabaseArgs hmDatabaseArgs;

  public OMTableCreateResponse(@Nonnull OMResponse omResponse,
                               @Nonnull OmTableInfo omTableInfo, @Nonnull HmDatabaseArgs hmDatabaseArgs) {
    super(omResponse);
    this.omTableInfo = omTableInfo;
    this.hmDatabaseArgs = hmDatabaseArgs;
  }

  public OMTableCreateResponse(@Nonnull OMResponse omResponse,
                               @Nonnull OmTableInfo omTableInfo) {
    super(omResponse);
    this.omTableInfo = omTableInfo;
    this.hmDatabaseArgs = null;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public OMTableCreateResponse(@Nonnull OMResponse omResponse) {
    super(omResponse);
    checkStatusNotOK();
    omTableInfo = null;
    hmDatabaseArgs = null;
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    String dbTabletKey =
        omMetadataManager.getMetaTableKey(omTableInfo.getDatabaseName(),
                omTableInfo.getTableName());
    omMetadataManager.getMetaTable().putWithBatch(batchOperation,
            dbTabletKey, omTableInfo);
    // update volume usedCapacityInBytes
    if (hmDatabaseArgs != null) {
      omMetadataManager.getDatabaseTable().putWithBatch(batchOperation,
              omMetadataManager.getDatabaseKey(hmDatabaseArgs.getName()),
              hmDatabaseArgs);
    }
  }

  @Nullable
  public OmTableInfo getOmTableInfo() {
    return omTableInfo;
  }

}

