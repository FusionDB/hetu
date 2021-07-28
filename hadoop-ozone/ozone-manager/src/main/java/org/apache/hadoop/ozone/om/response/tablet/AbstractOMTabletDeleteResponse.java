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

package org.apache.hadoop.ozone.om.response.tablet;

import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmTabletInfo;
import org.apache.hadoop.ozone.om.helpers.OmTabletLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmTabletInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DELETED_TABLET;

/**
 * Base class for responses that need to move tablets from an arbitrary table to
 * the deleted tablet table.
 */
@CleanupTableInfo(cleanupTables = {DELETED_TABLET})
public abstract class AbstractOMTabletDeleteResponse extends OMClientResponse {

  private boolean isRatisEnabled;

  public AbstractOMTabletDeleteResponse(
      @Nonnull OMResponse omResponse, boolean isRatisEnabled) {

    super(omResponse);
    this.isRatisEnabled = isRatisEnabled;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public AbstractOMTabletDeleteResponse(@Nonnull OMResponse omResponse) {
    super(omResponse);
    checkStatusNotOK();
  }

  /**
   * Adds the operation of deleting the {@code tabletName omTabletInfo} pair from
   * {@code fromTablet} to the batch operation {@code batchOperation}. The
   * batch operation is not committed, so no changes are persisted to disk.
   * The log transaction index used will be retrieved by calling
   * {@link OmTabletInfo#getUpdateID} on {@code omTabletInfo}.
   */
  protected void addDeletionToBatch(
          OMMetadataManager omMetadataManager,
          BatchOperation batchOperation,
          Table<String, ?> fromTable,
          String tabletName,
          OmTabletInfo omTabletInfo) throws IOException {

    // For OmResponse with failure, this should do nothing. This method is
    // not called in failure scenario in OM code.
    fromTable.deleteWithBatch(batchOperation, tabletName);

    // If tablet is not empty add this to delete table.
    if (!isTabletEmpty(omTabletInfo)) {
      // If a deleted tablet is put in the table where a tablet with the same
      // name already exists, then the old deleted tablet information would be
      // lost. To avoid this, first check if a tablet with same name exists.
      // deletedTable in OM Metadata stores <tabletName, RepeatedOMTabletInfo>.
      // The RepeatedOmTabletInfo is the structure that allows us to store a
      // list of OmTabletInfo that can be tied to same tablet name. For a tabletName
      // if RepeatedOMTabletInfo structure is null, we create a new instance,
      // if it is not null, then we simply add to the list and store this
      // instance in deletedTable.
      RepeatedOmTabletInfo repeatedOmTabletInfo =
              omMetadataManager.getDeletedTablet().get(tabletName);
      repeatedOmTabletInfo = OmUtils.prepareTabletForDelete(
              omTabletInfo, repeatedOmTabletInfo, omTabletInfo.getUpdateID(),
              isRatisEnabled);
      omMetadataManager.getDeletedTablet().putWithBatch(
              batchOperation, tabletName, repeatedOmTabletInfo);
    }
  }

  @Override
  public abstract void addToDBBatch(OMMetadataManager omMetadataManager,
        BatchOperation batchOperation) throws IOException;

  /**
   * Check if the tablet is empty or not. Tablet will be empty if it does not have
   * blocks.
   *
   * @param tabletInfo
   * @return if empty true, else false.
   */
  private boolean isTabletEmpty(@Nullable OmTabletInfo tabletInfo) {
    if (tabletInfo == null) {
      return true;
    }
    for (OmTabletLocationInfoGroup tabletLocationList : tabletInfo
            .getTabletLocationVersions()) {
      if (tabletLocationList.getLocationListCount() != 0) {
        return false;
      }
    }
    return true;
  }
}
