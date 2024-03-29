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
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om.ratis.utils;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.HAUtils;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.codec.OMDBDefinition;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.ozone.om.request.bucket.OMBucketCreateRequest;
import org.apache.hadoop.ozone.om.request.bucket.OMBucketDeleteRequest;
import org.apache.hadoop.ozone.om.request.bucket.OMBucketSetPropertyRequest;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.bucket.acl.OMBucketAddAclRequest;
import org.apache.hadoop.ozone.om.request.bucket.acl.OMBucketRemoveAclRequest;
import org.apache.hadoop.ozone.om.request.bucket.acl.OMBucketSetAclRequest;
import org.apache.hadoop.ozone.om.request.database.OMDatabaseCreateRequest;
import org.apache.hadoop.ozone.om.request.database.OMDatabaseDeleteRequest;
import org.apache.hadoop.ozone.om.request.file.OMDirectoryCreateRequest;
import org.apache.hadoop.ozone.om.request.file.OMFileCreateRequest;
import org.apache.hadoop.ozone.om.request.key.OMKeysDeleteRequest;
import org.apache.hadoop.ozone.om.request.key.OMAllocateBlockRequest;
import org.apache.hadoop.ozone.om.request.key.OMKeyCommitRequest;
import org.apache.hadoop.ozone.om.request.key.OMKeyCreateRequest;
import org.apache.hadoop.ozone.om.request.key.OMKeyDeleteRequest;
import org.apache.hadoop.ozone.om.request.key.OMKeyPurgeRequest;
import org.apache.hadoop.ozone.om.request.key.OMKeyRenameRequest;
import org.apache.hadoop.ozone.om.request.key.OMKeysRenameRequest;
import org.apache.hadoop.ozone.om.request.key.OMTrashRecoverRequest;
import org.apache.hadoop.ozone.om.request.key.acl.OMKeyAddAclRequest;
import org.apache.hadoop.ozone.om.request.key.acl.OMKeyRemoveAclRequest;
import org.apache.hadoop.ozone.om.request.key.acl.OMKeySetAclRequest;
import org.apache.hadoop.ozone.om.request.key.acl.prefix.OMPrefixAddAclRequest;
import org.apache.hadoop.ozone.om.request.key.acl.prefix.OMPrefixRemoveAclRequest;
import org.apache.hadoop.ozone.om.request.key.acl.prefix.OMPrefixSetAclRequest;
import org.apache.hadoop.ozone.om.request.s3.multipart.S3InitiateMultipartUploadRequest;
import org.apache.hadoop.ozone.om.request.s3.multipart.S3MultipartUploadAbortRequest;
import org.apache.hadoop.ozone.om.request.s3.multipart.S3MultipartUploadCommitPartRequest;
import org.apache.hadoop.ozone.om.request.s3.multipart.S3MultipartUploadCompleteRequest;
import org.apache.hadoop.ozone.om.request.s3.security.S3GetSecretRequest;
import org.apache.hadoop.ozone.om.request.s3.security.S3RevokeSecretRequest;
import org.apache.hadoop.ozone.om.request.security.OMCancelDelegationTokenRequest;
import org.apache.hadoop.ozone.om.request.security.OMGetDelegationTokenRequest;
import org.apache.hadoop.ozone.om.request.security.OMRenewDelegationTokenRequest;
import org.apache.hadoop.ozone.om.request.table.OMTableCreateRequest;
import org.apache.hadoop.ozone.om.request.table.OMTableDeleteRequest;
import org.apache.hadoop.ozone.om.request.table.OMTableSetPropertyRequest;
import org.apache.hadoop.ozone.om.request.volume.OMVolumeCreateRequest;
import org.apache.hadoop.ozone.om.request.volume.OMVolumeDeleteRequest;
import org.apache.hadoop.ozone.om.request.volume.OMVolumeSetOwnerRequest;
import org.apache.hadoop.ozone.om.request.volume.OMVolumeSetQuotaRequest;
import org.apache.hadoop.ozone.om.request.volume.acl.OMVolumeAddAclRequest;
import org.apache.hadoop.ozone.om.request.volume.acl.OMVolumeRemoveAclRequest;
import org.apache.hadoop.ozone.om.request.volume.acl.OMVolumeSetAclRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneObj.ObjectType;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.nio.file.Path;


/**
 * Utility class used by OzoneManager HA.
 */
public final class OzoneManagerRatisUtils {

  private OzoneManagerRatisUtils() {
  }
  /**
   * Create OMClientRequest which encapsulates the OMRequest.
   * @param omRequest
   * @return OMClientRequest
   * @throws IOException
   */
  public static OMClientRequest createClientRequest(OMRequest omRequest) {
    Type cmdType = omRequest.getCmdType();
    switch (cmdType) {
    case CreateVolume:
      return new OMVolumeCreateRequest(omRequest);
    case CreateDatabase:
      return new OMDatabaseCreateRequest(omRequest);
    case DeleteDatabase:
        return new OMDatabaseDeleteRequest(omRequest);
//    case SetDatabaseProperty:
//        return new OMSetDatabasePropertyRequest(omRequest);
    case SetVolumeProperty:
      boolean hasQuota = omRequest.getSetVolumePropertyRequest()
          .hasQuotaInBytes();
      boolean hasOwner = omRequest.getSetVolumePropertyRequest().hasOwnerName();
      Preconditions.checkState(hasOwner || hasQuota, "Either Quota or owner " +
          "should be set in the SetVolumeProperty request");
      Preconditions.checkState(!(hasOwner && hasQuota), "Either Quota or " +
          "owner should be set in the SetVolumeProperty request. Should not " +
          "set both");
      if (hasQuota) {
        return new OMVolumeSetQuotaRequest(omRequest);
      } else {
        return new OMVolumeSetOwnerRequest(omRequest);
      }
    case DeleteVolume:
      return new OMVolumeDeleteRequest(omRequest);
    case CreateBucket:
      return new OMBucketCreateRequest(omRequest);
    case DeleteBucket:
      return new OMBucketDeleteRequest(omRequest);
    case SetBucketProperty:
      return new OMBucketSetPropertyRequest(omRequest);
    case CreateTable:
      return new OMTableCreateRequest(omRequest);
    case DeleteTable:
      return new OMTableDeleteRequest(omRequest);
    case SetTableProperty:
      return new OMTableSetPropertyRequest(omRequest);
    case AllocateBlock:
      return new OMAllocateBlockRequest(omRequest);
    case CreateKey:
      return new OMKeyCreateRequest(omRequest);
    case CommitKey:
      return new OMKeyCommitRequest(omRequest);
    case DeleteKey:
      return new OMKeyDeleteRequest(omRequest);
    case DeleteKeys:
      return new OMKeysDeleteRequest(omRequest);
    case RenameKey:
      return new OMKeyRenameRequest(omRequest);
    case RenameKeys:
      return new OMKeysRenameRequest(omRequest);
    case CreateDirectory:
      return new OMDirectoryCreateRequest(omRequest);
    case CreateFile:
      return new OMFileCreateRequest(omRequest);
    case PurgeKeys:
      return new OMKeyPurgeRequest(omRequest);
    case InitiateMultiPartUpload:
      return new S3InitiateMultipartUploadRequest(omRequest);
    case CommitMultiPartUpload:
      return new S3MultipartUploadCommitPartRequest(omRequest);
    case AbortMultiPartUpload:
      return new S3MultipartUploadAbortRequest(omRequest);
    case CompleteMultiPartUpload:
      return new S3MultipartUploadCompleteRequest(omRequest);
    case AddAcl:
    case RemoveAcl:
    case SetAcl:
      return getOMAclRequest(omRequest);
    case GetDelegationToken:
      return new OMGetDelegationTokenRequest(omRequest);
    case CancelDelegationToken:
      return new OMCancelDelegationTokenRequest(omRequest);
    case RenewDelegationToken:
      return new OMRenewDelegationTokenRequest(omRequest);
    case GetS3Secret:
      return new S3GetSecretRequest(omRequest);
    case RecoverTrash:
      return new OMTrashRecoverRequest(omRequest);
    case RevokeS3Secret:
      return new S3RevokeSecretRequest(omRequest);
    default:
      throw new IllegalStateException("Unrecognized write command " +
          "type request" + cmdType);
    }
  }

  private static OMClientRequest getOMAclRequest(OMRequest omRequest) {
    Type cmdType = omRequest.getCmdType();
    if (Type.AddAcl == cmdType) {
      ObjectType type = omRequest.getAddAclRequest().getObj().getResType();
      if (ObjectType.VOLUME == type) {
        return new OMVolumeAddAclRequest(omRequest);
      } else if (ObjectType.BUCKET == type) {
        return new OMBucketAddAclRequest(omRequest);
      } else if (ObjectType.KEY == type) {
        return new OMKeyAddAclRequest(omRequest);
      } else {
        return new OMPrefixAddAclRequest(omRequest);
      }
    } else if (Type.RemoveAcl == cmdType) {
      ObjectType type = omRequest.getRemoveAclRequest().getObj().getResType();
      if (ObjectType.VOLUME == type) {
        return new OMVolumeRemoveAclRequest(omRequest);
      } else if (ObjectType.BUCKET == type) {
        return new OMBucketRemoveAclRequest(omRequest);
      } else if (ObjectType.KEY == type) {
        return new OMKeyRemoveAclRequest(omRequest);
      } else {
        return new OMPrefixRemoveAclRequest(omRequest);
      }
    } else {
      ObjectType type = omRequest.getSetAclRequest().getObj().getResType();
      if (ObjectType.VOLUME == type) {
        return new OMVolumeSetAclRequest(omRequest);
      } else if (ObjectType.BUCKET == type) {
        return new OMBucketSetAclRequest(omRequest);
      } else if (ObjectType.KEY == type) {
        return new OMKeySetAclRequest(omRequest);
      } else {
        return new OMPrefixSetAclRequest(omRequest);
      }
    }
  }

  /**
   * Convert exception result to {@link OzoneManagerProtocolProtos.Status}.
   * @param exception
   * @return OzoneManagerProtocolProtos.Status
   */
  public static Status exceptionToResponseStatus(IOException exception) {
    if (exception instanceof OMException) {
      return Status.values()[((OMException) exception).getResult().ordinal()];
    } else {
      // Doing this here, because when DB error happens we need to return
      // correct error code, so that in applyTransaction we can
      // completeExceptionally for DB errors.

      // Currently Table API's are in hdds-common, which are used by SCM, OM
      // currently. So, they return IOException with setting cause to
      // RocksDBException. So, here that is the reason for the instanceof
      // check RocksDBException.
      if (exception.getCause() != null
          && exception.getCause() instanceof RocksDBException) {
        return Status.METADATA_ERROR;
      } else {
        return Status.INTERNAL_ERROR;
      }
    }
  }

  /**
   * Obtain OMTransactionInfo from Checkpoint.
   */
  public static TransactionInfo getTrxnInfoFromCheckpoint(
      OzoneConfiguration conf, Path dbPath) throws Exception {
    return HAUtils
        .getTrxnInfoFromCheckpoint(conf, dbPath, new OMDBDefinition());
  }

  /**
   * Verify transaction info with provided lastAppliedIndex.
   *
   * If transaction info transaction Index is less than or equal to
   * lastAppliedIndex, return false, else return true.
   * @param transactionInfo
   * @param lastAppliedIndex
   * @param leaderId
   * @param newDBlocation
   * @return boolean
   */
  public static boolean verifyTransactionInfo(TransactionInfo transactionInfo,
      long lastAppliedIndex, String leaderId, Path newDBlocation) {
    return HAUtils
        .verifyTransactionInfo(transactionInfo, lastAppliedIndex, leaderId,
            newDBlocation, OzoneManager.LOG);
  }
}
