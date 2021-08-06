/*
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

package org.apache.hadoop.hetu.om.response.tablet;

import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.hetu.om.response.CleanupTableInfo;
import org.apache.hadoop.hetu.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.List;

import static org.apache.hadoop.hetu.om.OmMetadataManagerImpl.DELETED_TABLET;

/**
 * Response for {@link OMTabletPurgeResponse} request.
 */
@CleanupTableInfo(cleanupTables = {DELETED_TABLET})
public class OMTabletPurgeResponse extends OMClientResponse {
  private List<String> purgeTabletList;

  public OMTabletPurgeResponse(@Nonnull OMResponse omResponse,
                               @Nonnull List<String> tabletList) {
    super(omResponse);
    this.purgeTabletList = tabletList;
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    for (String tablet : purgeTabletList) {
      omMetadataManager.getDeletedTablet().deleteWithBatch(batchOperation,
          tablet);
    }
  }

}
