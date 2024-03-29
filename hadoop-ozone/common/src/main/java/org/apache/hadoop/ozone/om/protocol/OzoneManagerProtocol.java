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

package org.apache.hadoop.ozone.om.protocol;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.hm.HmDatabaseArgs;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.DBUpdates;
import org.apache.hadoop.ozone.om.helpers.OmBucketArgs;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDeleteKeys;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartCommitUploadPartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadCompleteInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadCompleteList;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadList;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadListParts;
import org.apache.hadoop.ozone.om.helpers.OmRenameKeys;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.om.helpers.ServiceInfo;
import org.apache.hadoop.ozone.om.helpers.ServiceInfoEx;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneAclInfo;
import org.apache.hadoop.ozone.security.OzoneDelegationTokenSelector;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.token.TokenInfo;

/**
 * Protocol to talk to OM.
 */
@KerberosInfo(
    serverPrincipal = OMConfigKeys.OZONE_OM_KERBEROS_PRINCIPAL_KEY)
@TokenInfo(OzoneDelegationTokenSelector.class)
public interface OzoneManagerProtocol
    extends OzoneManagerSecurityProtocol, Closeable {

  @SuppressWarnings("checkstyle:ConstantName")
  /**
   * Version 1: Initial version.
   */
  long versionID = 1L;

  /**
   * Creates a volume.
   * @param args - Arguments to create Volume.
   * @throws IOException
   */
  void createVolume(OmVolumeArgs args) throws IOException;

  /**
   * Changes the owner of a volume.
   * @param volume  - Name of the volume.
   * @param owner - Name of the owner.
   * @return true if operation succeeded, false if specified user is
   *         already the owner.
   * @throws IOException
   */
  boolean setOwner(String volume, String owner) throws IOException;

  /**
   * Changes the Quota on a volume.
   * @param volume - Name of the volume.
   * @param quotaInNamespace - Volume quota in counts.
   * @param quotaInBytes - Volume quota in bytes.
   * @throws IOException
   */
  void setQuota(String volume, long quotaInNamespace, long quotaInBytes)
      throws IOException;

  /**
   * Checks if the specified user can access this volume.
   * @param volume - volume
   * @param userAcl - user acls which needs to be checked for access
   * @return true if the user has required access for the volume,
   *         false otherwise
   * @throws IOException
   */
  boolean checkVolumeAccess(String volume, OzoneAclInfo userAcl)
      throws IOException;

  /**
   * Gets the volume information.
   * @param volume - Volume name.
   * @return VolumeArgs or exception is thrown.
   * @throws IOException
   */
  OmVolumeArgs getVolumeInfo(String volume) throws IOException;

  /**
   * Deletes an existing empty volume.
   * @param volume - Name of the volume.
   * @throws IOException
   */
  void deleteVolume(String volume) throws IOException;

  /**
   * Lists volumes accessible by a specific user.
   * @param userName - user name
   * @param prefix  - Filter prefix -- Return only entries that match this.
   * @param prevKey - Previous key -- List starts from the next from the prevkey
   * @param maxKeys - Max number of keys to return.
   * @return List of Volumes.
   * @throws IOException
   */
  List<OmVolumeArgs> listVolumeByUser(String userName, String prefix, String
      prevKey, int maxKeys) throws IOException;

  /**
   * Lists volume all volumes in the cluster.
   * @param prefix  - Filter prefix -- Return only entries that match this.
   * @param prevKey - Previous key -- List starts from the next from the prevkey
   * @param maxKeys - Max number of keys to return.
   * @return List of Volumes.
   * @throws IOException
   */
  List<OmVolumeArgs> listAllVolumes(String prefix, String
      prevKey, int maxKeys) throws IOException;

  /**
   * Creates a bucket.
   * @param bucketInfo - BucketInfo to create Bucket.
   * @throws IOException
   */
  void createBucket(OmBucketInfo bucketInfo) throws IOException;

  /**
   * Gets the bucket information.
   * @param volumeName - Volume name.
   * @param bucketName - Bucket name.
   * @return OmBucketInfo or exception is thrown.
   * @throws IOException
   */
  OmBucketInfo getBucketInfo(String volumeName, String bucketName)
      throws IOException;

  /**
   * Sets bucket property from args.
   * @param args - BucketArgs.
   * @throws IOException
   */
  void setBucketProperty(OmBucketArgs args) throws IOException;

  /**
   * Open the given key and return an open key session.
   *
   * @param args the args of the key.
   * @return OpenKeySession instance that client uses to talk to container.
   * @throws IOException
   */
  OpenKeySession openKey(OmKeyArgs args) throws IOException;

  /**
   * Commit a key. This will make the change from the client visible. The client
   * is identified by the clientID.
   *
   * @param args the key to commit
   * @param clientID the client identification
   * @throws IOException
   */
  void commitKey(OmKeyArgs args, long clientID) throws IOException;

  /**
   * Allocate a new block, it is assumed that the client is having an open key
   * session going on. This block will be appended to this open key session.
   *
   * @param args the key to append
   * @param clientID the client identification
   * @param excludeList List of datanodes/containers to exclude during block
   *                    allocation
   * @return an allocated block
   * @throws IOException
   */
  OmKeyLocationInfo allocateBlock(OmKeyArgs args, long clientID,
      ExcludeList excludeList) throws IOException;


  /**
   * Look up for the container of an existing key.
   *
   * @param args the args of the key.
   * @return OmKeyInfo instance that client uses to talk to container.
   * @throws IOException
   */
  OmKeyInfo lookupKey(OmKeyArgs args) throws IOException;

  /**
   * Rename an existing key within a bucket.
   * @param args the args of the key.
   * @param toKeyName New name to be used for the Key
   * @throws IOException
   */
  void renameKey(OmKeyArgs args, String toKeyName) throws IOException;

  /**
   * Rename existing keys within a bucket.
   * @param omRenameKeys Includes volume, bucket, and fromKey toKey name map
   *                     and fromKey name toKey info Map.
   * @throws IOException
   */
  void renameKeys(OmRenameKeys omRenameKeys) throws IOException;

  /**
   * Deletes an existing key.
   *
   * @param args the args of the key.
   * @throws IOException
   */
  void deleteKey(OmKeyArgs args) throws IOException;

  /**
   * Deletes existing key/keys. This interface supports delete
   * multiple keys and a single key. Used by deleting files
   * through OzoneFileSystem.
   *
   * @param deleteKeys
   * @throws IOException
   */
  void deleteKeys(OmDeleteKeys deleteKeys) throws IOException;

  /**
   * Deletes an existing empty bucket from volume.
   * @param volume - Name of the volume.
   * @param bucket - Name of the bucket.
   * @throws IOException
   */
  void deleteBucket(String volume, String bucket) throws IOException;

  /**
   * Returns a list of buckets represented by {@link OmBucketInfo}
   * in the given volume. Argument volumeName is required, others
   * are optional.
   *
   * @param volumeName
   *   the name of the volume.
   * @param startBucketName
   *   the start bucket name, only the buckets whose name is
   *   after this value will be included in the result.
   * @param bucketPrefix
   *   bucket name prefix, only the buckets whose name has
   *   this prefix will be included in the result.
   * @param maxNumOfBuckets
   *   the maximum number of buckets to return. It ensures
   *   the size of the result will not exceed this limit.
   * @return a list of buckets.
   * @throws IOException
   */
  List<OmBucketInfo> listBuckets(String volumeName,
      String startBucketName, String bucketPrefix, int maxNumOfBuckets)
      throws IOException;

  /**
   * Returns a list of keys represented by {@link OmKeyInfo}
   * in the given bucket. Argument volumeName, bucketName is required,
   * others are optional.
   *
   * @param volumeName
   *   the name of the volume.
   * @param bucketName
   *   the name of the bucket.
   * @param startKeyName
   *   the start key name, only the keys whose name is
   *   after this value will be included in the result.
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
      String bucketName, String startKeyName, String keyPrefix, int maxKeys)
      throws IOException;

  /**
   * Returns list of Ozone services with its configuration details.
   *
   * @return list of Ozone services
   * @throws IOException
   */
  List<ServiceInfo> getServiceList() throws IOException;

  ServiceInfoEx getServiceInfo() throws IOException;

  /*
   * S3 Specific functionality that is supported by Ozone Manager.
   */

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
  OmMultipartUploadCompleteInfo completeMultipartUpload(
      OmKeyArgs omKeyArgs, OmMultipartUploadCompleteList multipartUploadList)
      throws IOException;

  /**
   * Abort multipart upload.
   * @param omKeyArgs
   * @throws IOException
   */
  void abortMultipartUpload(OmKeyArgs omKeyArgs) throws IOException;

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
   * List in-flight uploads.
   */
  OmMultipartUploadList listMultipartUploads(String volumeName,
      String bucketName, String prefix) throws IOException;

  /**
   * Gets s3Secret for given kerberos user.
   * @param kerberosID
   * @return S3SecretValue
   * @throws IOException
   */
  S3SecretValue getS3Secret(String kerberosID) throws IOException;

  /**
   * Revokes s3Secret of given kerberos user.
   * @param kerberosID
   * @throws IOException
   */
  void revokeS3Secret(String kerberosID) throws IOException;

  /**
   * OzoneFS api to get file status for an entry.
   *
   * @param keyArgs Key args
   * @throws OMException if file does not exist
   *                     if bucket does not exist
   * @throws IOException if there is error in the db
   *                     invalid arguments
   */
  OzoneFileStatus getFileStatus(OmKeyArgs keyArgs) throws IOException;

  /**
   * Ozone FS api to create a directory. Parent directories if do not exist
   * are created for the input directory.
   *
   * @param args Key args
   * @throws OMException if any entry in the path exists as a file
   *                     if bucket does not exist
   * @throws IOException if there is error in the db
   *                     invalid arguments
   */
  void createDirectory(OmKeyArgs args) throws IOException;

  /**
   * OzoneFS api to creates an output stream for a file.
   *
   * @param keyArgs   Key args
   * @param overWrite if true existing file at the location will be overwritten
   * @param recursive if true file would be created even if parent directories
   *                  do not exist
   * @throws OMException if given key is a directory
   *                     if file exists and isOverwrite flag is false
   *                     if an ancestor exists as a file
   *                     if bucket does not exist
   * @throws IOException if there is error in the db
   *                     invalid arguments
   */
  OpenKeySession createFile(OmKeyArgs keyArgs, boolean overWrite,
      boolean recursive) throws IOException;

  /**
   * OzoneFS api to lookup for a file.
   *
   * @param keyArgs Key args
   * @throws OMException if given key is not found or it is not a file
   *                     if bucket does not exist
   * @throws IOException if there is error in the db
   *                     invalid arguments
   */
  OmKeyInfo lookupFile(OmKeyArgs keyArgs) throws IOException;

  /**
   * List the status for a file or a directory and its contents.
   *
   * @param keyArgs    Key args
   * @param recursive  For a directory if true all the descendants of a
   *                   particular directory are listed
   * @param startKey   Key from which listing needs to start. If startKey exists
   *                   its status is included in the final list.
   * @param numEntries Number of entries to list from the start key
   * @return list of file status
   */
  List<OzoneFileStatus> listStatus(OmKeyArgs keyArgs, boolean recursive,
      String startKey, long numEntries) throws IOException;

  /**
   * Add acl for Ozone object. Return true if acl is added successfully else
   * false.
   * @param obj Ozone object for which acl should be added.
   * @param acl ozone acl to be added.
   *
   * @throws IOException if there is error.
   * */
  boolean addAcl(OzoneObj obj, OzoneAcl acl) throws IOException;

  /**
   * Remove acl for Ozone object. Return true if acl is removed successfully
   * else false.
   * @param obj Ozone object.
   * @param acl Ozone acl to be removed.
   *
   * @throws IOException if there is error.
   * */
  boolean removeAcl(OzoneObj obj, OzoneAcl acl) throws IOException;

  /**
   * Acls to be set for given Ozone object. This operations reset ACL for
   * given object to list of ACLs provided in argument.
   * @param obj Ozone object.
   * @param acls List of acls.
   *
   * @throws IOException if there is error.
   * */
  boolean setAcl(OzoneObj obj, List<OzoneAcl> acls) throws IOException;

  /**
   * Returns list of ACLs for given Ozone object.
   * @param obj Ozone object.
   *
   * @throws IOException if there is error.
   * */
  List<OzoneAcl> getAcl(OzoneObj obj) throws IOException;

  /**
   * Get DB updates since a specific sequence number.
   * @param dbUpdatesRequest request that encapsulates a sequence number.
   * @return Wrapper containing the updates.
   */
  DBUpdates getDBUpdates(
      OzoneManagerProtocolProtos.DBUpdatesRequest dbUpdatesRequest)
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
   * Recover trash allows the user to recover keys that were marked as deleted,
   * but not actually deleted by Ozone Manager.
   * @param volumeName - The volume name.
   * @param bucketName - The bucket name.
   * @param keyName - The key user want to recover.
   * @param destinationBucket - The bucket user want to recover to.
   * @return The result of recovering operation is success or not.
   * @throws IOException
   */
  default boolean recoverTrash(String volumeName, String bucketName,
      String keyName, String destinationBucket) throws IOException {
    return false;
  }

  void createDatabase(HmDatabaseArgs build) throws IOException;

  HmDatabaseArgs getDatabaseInfo(String databaseName) throws IOException;

  void deleteDatabase(String databaseName) throws IOException;

  List<HmDatabaseArgs> listAllDatabases(String databasePrefix, String prevDatabase, int maxListResult) throws IOException;

  List<HmDatabaseArgs> listDatabaseByUser(String user, String databasePrefix, String prevDatabase, int maxListResult) throws IOException;
}
