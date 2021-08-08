/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hetu.om;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hetu.om.block.OzoneManagerTablet;
import org.apache.hadoop.hetu.om.fs.OzoneManagerFS;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartCommitUploadPartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadCompleteInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadCompleteList;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadList;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadListParts;
import org.apache.hadoop.ozone.om.helpers.OmTabletArgs;
import org.apache.hadoop.ozone.om.helpers.OmTabletInfo;
import org.apache.hadoop.ozone.om.helpers.OmTabletLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.helpers.OpenTabletSession;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmTabletInfo;

import java.io.IOException;
import java.util.List;

/**
 * Handles tablet level commands.
 */
public interface TabletManager extends OzoneManagerTablet, IOzoneAcl {

  /**
   * Start tablet manager.
   *
   * @param configuration
   * @throws IOException
   */
  void start(OzoneConfiguration configuration);

  /**
   * Stop tablet manager.
   */
  void stop() throws IOException;

  /**
   * After calling commit, the tablet will be made visible. There can be multiple
   * open tablet writes in parallel (identified by client id). The most recently
   * committed one will be the one visible.
   *
   * @param args the tablet to commit.
   * @param clientID the client that is committing.
   * @throws IOException
   */
  void commitTablet(OmTabletArgs args, long clientID) throws IOException;

  /**
   * A client calls this on an open tablet, to request to allocate a new block,
   * and appended to the tail of current block list of the open client.
   *
   * @param args the tablet to append
   * @param clientID the client requesting block.
   * @param excludeList List of datanodes/containers to exclude during block
   *                    allocation.
   * @return the reference to the new block.
   * @throws IOException
   */
  OmTabletLocationInfo allocateTablet(OmTabletArgs args, long clientID,
    ExcludeList excludeList) throws IOException;

  /**
   * Given the args of a tablet to put, write an open tablet entry to meta data.
   *
   * In case that the container creation or tablet write failed on
   * DistributedStorageHandler, this tablet's metadata will still stay in OM.
   * TODO garbage collect the open tablets that never get closed
   *
   * @param args the args of the tablet provided by client.
   * @return a OpenTabletSession instance client uses to talk to container.
   * @throws IOException
   */
  OpenTabletSession openTablet(OmTabletArgs args) throws IOException;

  /**
   * Look up an existing tablet. Return the info of the tablet to client side, which
   * DistributedStorageHandler will use to access the data on datanode.
   *
   * @param args the args of the tablet provided by client.
   * @param clientAddress a hint to tablet manager, order the datanode in returned
   *                      pipeline by distance between client and datanode.
   * @return a OmTabletInfo instance client uses to talk to container.
   * @throws IOException
   */
  OmTabletInfo lookupTablet(OmTabletArgs args, String clientAddress) throws IOException;

  /**
   * Renames an existing tablet within a bucket.
   *
   * @param args the args of the tablet provided by client.
   * @param toTabletName New name to be used for the tablet
   * @throws IOException if specified tablet doesn't exist or
   * some other I/O errors while renaming the tablet.
   */
  void renameTablet(OmTabletArgs args, String toTabletName) throws IOException;

  /**
   * Deletes an object by an object tablet. The tablet will be immediately removed
   * from OM namespace and become invisible to clients. The object data
   * will be removed in async manner that might retain for some time.
   *
   * @param args the args of the tablet provided by client.
   * @throws IOException if specified tablet doesn't exist or
   * some other I/O errors while deleting an object.
   */
  void deleteTablet(OmTabletArgs args) throws IOException;

  /**
   * Returns a list of tablets represented by {@link OmTabletInfo}
   * in the given partition.
   *
   * @param databaseName
   *   the name of the database.
   * @param tableName
   *   the name of the table.
   * @param partitionName
   *   the name of the partition.
   * @param startTablet
   *   the start tablet name, only the tablets whose name is
   *   after this value will be included in the result.
   *   This tablet is excluded from the result.
   * @param tabletPrefix
   *   tablet name prefix, only the tablets whose name has
   *   this prefix will be included in the result.
   * @param maxTablets
   *   the maximum number of tablets to return. It ensures
   *   the size of the result will not exceed this limit.
   * @return a list of tablets.
   * @throws IOException
   */
  List<OmTabletInfo> listTablets(String databaseName, String tableName,
      String partitionName, String startTablet, String tabletPrefix, int maxTablets)
      throws IOException;

  /**
   * List trash allows the user to list the tablets that were marked as deleted,
   * but not actually deleted by Ozone Manager. This allows a user to recover
   * tablets within a configurable window.
   * @param databaseName - The database name, which can also be a wild card
   *                   using '*'.
   * @param tableName - The table name, which can also be a wild card
   *                   using '*'.
   * @param partitionName - The partition name, which can also be a wild card
   *                   using '*'.
   * @param startTablet - List tablets from a specific tablet name.
   * @param tabletPrefix - List tablets using a specific prefix.
   * @param maxTablets - The number of tablets to be returned. This must be below
   *                the cluster level set by admins.
   * @return The list of tablets that are deleted from the deleted table.
   * @throws IOException
   */
  List<RepeatedOmTabletInfo> listTrash(String databaseName, String tableName, String partitionName,
      String startTablet, String tabletPrefix, int maxTablets) throws IOException;

  /**
   * Returns a list of pending deletion tablet info that ups to the given count.
   * Each entry is a {@link BlockGroup}, which contains the info about the
   * tablet name and all its associated block IDs. A pending deletion tablet is
   * stored with #deleting# prefix in OM DB.
   *
   * @param count max number of tablets to return.
   * @return a list of {@link BlockGroup} representing tablets and blocks.
   * @throws IOException
   */
  List<BlockGroup> getPendingDeletionTablets(int count) throws IOException;

  /**
   * Returns the names of up to {@code count} open tablets that are older than
   * the configured expiration age.
   *
   * @param count The maximum number of expired open tablets to return.
   * @return a list of {@link String} representing the names of expired
   * open tablets.
   * @throws IOException
   */
  List<String> getExpiredOpenTablets(int count) throws IOException;

  /**
   * Deletes a expired open tablet by its name. Called when a hanging tablet has been
   * lingering for too long. Once called, the open tablet entries gets removed
   * from OM mdata data.
   *
   * @param objectTabletName object tablet name with #open# prefix.
   * @throws IOException if specified tablet doesn't exist or other I/O errors.
   */
  void deleteExpiredOpenTablet(String objectTabletName) throws IOException;

  /**
   * Returns the metadataManager.
   * @return OMMetadataManager.
   */
  OMMetadataManager getMetadataManager();

  /**
   * Returns the instance of Deleting Service.
   * @return Background service.
   */
  BackgroundService getDeletingService();

  /**
   * Refresh the tablet block location information by get latest info from SCM.
   * @param tablet
   */
  void refresh(OmTabletInfo tablet) throws IOException;
}
