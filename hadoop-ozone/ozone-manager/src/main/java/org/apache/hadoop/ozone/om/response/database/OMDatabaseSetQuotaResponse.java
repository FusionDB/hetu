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

import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hetu.hm.helpers.OmDatabaseArgs;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

import javax.annotation.Nonnull;
import java.io.IOException;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DATABASE_TABLE;

/**
 * Response for set quota request.
 */
@CleanupTableInfo(cleanupTables = {DATABASE_TABLE})
public class OMDatabaseSetQuotaResponse extends OMClientResponse {
  private OmDatabaseArgs omDatabaseArgs;

  public OMDatabaseSetQuotaResponse(@Nonnull OMResponse omResponse,
                                    @Nonnull OmDatabaseArgs omDatabaseArgs) {
    super(omResponse);
    this.omDatabaseArgs = omDatabaseArgs;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public OMDatabaseSetQuotaResponse(@Nonnull OMResponse omResponse) {
    super(omResponse);
    checkStatusNotOK();
  }

  @Override
  protected void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    omMetadataManager.getDatabaseTable().putWithBatch(batchOperation,
        omMetadataManager.getDatabaseKey(omDatabaseArgs.getName()),
            omDatabaseArgs);
  }

}
