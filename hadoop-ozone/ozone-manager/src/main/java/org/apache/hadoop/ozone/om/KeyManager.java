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
package org.apache.hadoop.ozone.om;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.ozone.common.BlockGroup;
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
import org.apache.hadoop.ozone.om.helpers.OmTabletInfo;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.fs.OzoneManagerFS;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;

import java.io.IOException;
import java.util.List;

/**
 * Handles key level commands.
 */
public interface KeyManager extends OzoneManagerFS, IOzoneAcl {

  /**
   * Start key manager.
   *
   * @param configuration
   * @throws IOException
   */
  void start(OzoneConfiguration configuration);

  /**
   * Stop key manager.
   */
  void stop() throws IOException;

  /**
   * After calling commit, the key will be made visible. There can be multiple
   * open key writes in parallel (identified by client id). The most recently
   * committed one will be the one visible.
   *
   * @param args the key to commit.
   * @param clientID the client that is committing.
   * @throws IOException
   */
  void commitKey(OmKeyArgs args, long clientID) throws IOException;

  /**
   * A client calls this on an open key, to request to allocate a new block,
   * and appended to the tail of current block list of the open client.
   *
   * @param args the key to append
   * @param clientID the client requesting block.
   * @param excludeList List of datanodes/containers to exclude during block
   *                    allocation.
   * @return the reference to the new block.
   * @throws IOException
   */
  OmKeyLocationInfo allocateBlock(OmKeyArgs args, long clientID,
      ExcludeList excludeList) throws IOException;

  /**
   * Given the args of a key to put, write an open key entry to meta data.
   *
   * In case that the container creation or key write failed on
   * DistributedStorageHandler, this key's metadata will still stay in OM.
   * TODO garbage collect the open keys that never get closed
   *
   * @param args the args of the key provided by client.
   * @return a OpenKeySession instance client uses to talk to container.
   * @throws IOException
   */
  OpenKeySession openKey(OmKeyArgs args) throws IOException;

  /**
   * Look up an existing key. Return the info of the key to client side, which
   * DistributedStorageHandler will use to access the data on datanode.
   *
   * @param args the args of the key provided by client.
   * @param clientAddress a hint to key manager, order the datanode in returned
   *                      pipeline by distance between client and datanode.
   * @return a OmKeyInfo instance client uses to talk to container.
   * @throws IOException
   */
  OmKeyInfo lookupKey(OmKeyArgs args, String clientAddress) throws IOException;

  /**
   * Renames an existing key within a bucket.
   *
   * @param args the args of the key provided by client.
   * @param toKeyName New name to be used for the key
   * @throws IOException if specified key doesn't exist or
   * some other I/O errors while renaming the key.
   */
  void renameKey(OmKeyArgs args, String toKeyName) throws IOException;

  /**
   * Deletes an object by an object key. The key will be immediately removed
   * from OM namespace and become invisible to clients. The object data
   * will be removed in async manner that might retain for some time.
   *
   * @param args the args of the key provided by client.
   * @throws IOException if specified key doesn't exist or
   * some other I/O errors while deleting an object.
   */
  void deleteKey(OmKeyArgs args) throws IOException;

  /**
   * Returns a list of keys represented by {@link OmKeyInfo}
   * in the given bucket.
   *
   * @param volumeName
   *   the name of the volume.
   * @param bucketName
   *   the name of the bucket.
   * @param startKey
   *   the start key name, only the keys whose name is
   *   after this value will be included in the result.
   *   This key is excluded from the result.
   * @param keyPrefix
   *   key name prefix, only the keys whose name has
   *   this prefix will be included in the result.
   * @param maxKeys
   *   the maximum number of keys to return. It ensures
   *   the size of the result will not exceed this limit.
   * @return a list of keys.
   * @throws IOException
   */
  List<OmKeyInfo> listKeys(String volumeName,
      String bucketName, String startKey, String keyPrefix, int maxKeys)
      throws IOException;

  /**
   * List trash allows the user to list the keys that were marked as deleted,
   * but not actually deleted by Ozone Manager. This allows a user to recover
   * keys within a configurable window.
   * @param volumeName - The volume name, which can also be a wild card
   *                   using '*'.
   * @param bucketName - The bucket name, which can also be a wild card
   *                   using '*'.
   * @param startKeyName - List keys from a specific key name.
   * @param keyPrefix - List keys using a specific prefix.
   * @param maxKeys - The number of keys to be returned. This must be below
   *                the cluster level set by admins.
   * @return The list of keys that are deleted from the deleted table.
   * @throws IOException
   */
  List<RepeatedOmKeyInfo> listTrash(String volumeName, String bucketName,
      String startKeyName, String keyPrefix, int maxKeys) throws IOException;

  /**
   * Returns a list of pending deletion key info that ups to the given count.
   * Each entry is a {@link BlockGroup}, which contains the info about the
   * key name and all its associated block IDs. A pending deletion key is
   * stored with #deleting# prefix in OM DB.
   *
   * @param count max number of keys to return.
   * @return a list of {@link BlockGroup} representing keys and blocks.
   * @throws IOException
   */
  List<BlockGroup> getPendingDeletionKeys(int count) throws IOException;

  /**
   * Returns the names of up to {@code count} open keys that are older than
   * the configured expiration age.
   *
   * @param count The maximum number of expired open keys to return.
   * @return a list of {@link String} representing the names of expired
   * open keys.
   * @throws IOException
   */
  List<String> getExpiredOpenKeys(int count) throws IOException;

  /**
   * Deletes a expired open key by its name. Called when a hanging key has been
   * lingering for too long. Once called, the open key entries gets removed
   * from OM mdata data.
   *
   * @param objectKeyName object key name with #open# prefix.
   * @throws IOException if specified key doesn't exist or other I/O errors.
   */
  void deleteExpiredOpenKey(String objectKeyName) throws IOException;

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
   * Initiate multipart upload for the specified key.
   * @param keyArgs
   * @return MultipartInfo
   * @throws IOException
   */
  OmMultipartInfo initiateMultipartUpload(OmKeyArgs keyArgs) throws IOException;

  /**
   * Commit Multipart upload part file.
   * @param omKeyArgs
   * @param clientID
   * @return OmMultipartCommitUploadPartInfo
   * @throws IOException
   */

  OmMultipartCommitUploadPartInfo commitMultipartUploadPart(
      OmKeyArgs omKeyArgs, long clientID) throws IOException;

  /**
   * Complete Multipart upload Request.
   * @param omKeyArgs
   * @param multipartUploadList
   * @return OmMultipartUploadCompleteInfo
   * @throws IOException
   */
  OmMultipartUploadCompleteInfo completeMultipartUpload(OmKeyArgs omKeyArgs,
      OmMultipartUploadCompleteList multipartUploadList) throws IOException;

  /**
   * Abort multipart upload request.
   * @param omKeyArgs
   * @throws IOException
   */
  void abortMultipartUpload(OmKeyArgs omKeyArgs) throws IOException;

  OmMultipartUploadList listMultipartUploads(String volumeName,
      String bucketName, String prefix) throws OMException;

  /**
   * Returns list of parts of a multipart upload key.
   * @param volumeName
   * @param bucketName
   * @param keyName
   * @param uploadID
   * @param partNumberMarker
   * @param maxParts
   * @return OmMultipartUploadListParts
   */
  OmMultipartUploadListParts listParts(String volumeName, String bucketName,
      String keyName, String uploadID, int partNumberMarker,
      int maxParts)  throws IOException;

  /**
   * Refresh the key block location information by get latest info from SCM.
   * @param key
   */
  void refresh(OmKeyInfo key) throws IOException;

  /**
   * Refresh the tablet location information by get latest info from SCM.
   * @param tablet
   */
  void refresh(OmTabletInfo tablet) throws IOException;;
}
