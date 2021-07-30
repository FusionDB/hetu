/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.om;

import com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.utils.DBCheckpointMetrics;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;

/**
 * This class is for maintaining Ozone Manager statistics.
 */
@InterfaceAudience.Private
@Metrics(about="Ozone Manager Metrics", context="dfs")
public class OMMetrics {
  private static final String SOURCE_NAME =
      OMMetrics.class.getSimpleName();

  // OM request type op metrics
  private @Metric MutableCounterLong numVolumeOps;
  private @Metric MutableCounterLong numDatabaseOps;
  private @Metric MutableCounterLong numBucketOps;
  private @Metric MutableCounterLong numKeyOps;
  private @Metric MutableCounterLong numFSOps;
  private @Metric MutableCounterLong numTableOps;
  private @Metric MutableCounterLong numPartitionOps;
  private @Metric MutableCounterLong numTabletOps;

  // OM op metrics
  private @Metric MutableCounterLong numVolumeCreates;
  private @Metric MutableCounterLong numDatabaseCreates;
  private @Metric MutableCounterLong numVolumeUpdates;
  private @Metric MutableCounterLong numVolumeInfos;
  private @Metric MutableCounterLong numDatabaseInfos;
  private @Metric MutableCounterLong numVolumeCheckAccesses;
  private @Metric MutableCounterLong numBucketCreates;
  private @Metric MutableCounterLong numVolumeDeletes;
  private @Metric MutableCounterLong numDatabaseDeletes;
  private @Metric MutableCounterLong numBucketInfos;
  private @Metric MutableCounterLong numBucketUpdates;
  private @Metric MutableCounterLong numBucketDeletes;
  private @Metric MutableCounterLong numKeyAllocate;
  private @Metric MutableCounterLong numKeyLookup;
  private @Metric MutableCounterLong numKeyRenames;
  private @Metric MutableCounterLong numKeyDeletes;
  private @Metric MutableCounterLong numBucketLists;
  private @Metric MutableCounterLong numKeyLists;
  private @Metric MutableCounterLong numTrashKeyLists;
  private @Metric MutableCounterLong numVolumeLists;
  private @Metric MutableCounterLong numDatabaseLists;
  private @Metric MutableCounterLong numKeyCommits;
  private @Metric MutableCounterLong numBlockAllocations;
  private @Metric MutableCounterLong numGetServiceLists;
  private @Metric MutableCounterLong numBucketS3Lists;
  private @Metric MutableCounterLong numInitiateMultipartUploads;
  private @Metric MutableCounterLong numCompleteMultipartUploads;
  private @Metric MutableCounterLong numTableCreates;
  private @Metric MutableCounterLong numTabletDeletes;
  private @Metric MutableCounterLong numTableDeletes;
  private @Metric MutableCounterLong numTableUpdates;
  private @Metric MutableCounterLong numDatabaseUpdates;
  private @Metric MutableCounterLong numPartitionCreates;
  private @Metric MutableCounterLong numPartitionDeletes;
  private @Metric MutableCounterLong numPartitionUpdates;
  private @Metric MutableCounterLong numTabletAllocate;
  private @Metric MutableCounterLong numTabletCommits;


  private @Metric MutableCounterLong numGetFileStatus;
  private @Metric MutableCounterLong numCreateDirectory;
  private @Metric MutableCounterLong numCreateFile;
  private @Metric MutableCounterLong numLookupFile;
  private @Metric MutableCounterLong numListStatus;

  private @Metric MutableCounterLong numOpenKeyDeleteRequests;
  private @Metric MutableCounterLong numOpenKeysSubmittedForDeletion;
  private @Metric MutableCounterLong numOpenKeysDeleted;
  private @Metric MutableCounterLong numOpenTabletDeleteRequests;
  private @Metric MutableCounterLong numOpenTabletsSubmittedForDeletion;
  private @Metric MutableCounterLong numOpenTabletsDeleted;


  private @Metric MutableCounterLong numAddAcl;
  private @Metric MutableCounterLong numSetAcl;
  private @Metric MutableCounterLong numGetAcl;
  private @Metric MutableCounterLong numRemoveAcl;

  // Failure Metrics
  private @Metric MutableCounterLong numVolumeCreateFails;
  private @Metric MutableCounterLong numVolumeUpdateFails;
  private @Metric MutableCounterLong numVolumeInfoFails;
  private @Metric MutableCounterLong numVolumeDeleteFails;
  private @Metric MutableCounterLong numBucketCreateFails;
  private @Metric MutableCounterLong numVolumeCheckAccessFails;
  private @Metric MutableCounterLong numBucketInfoFails;
  private @Metric MutableCounterLong numBucketUpdateFails;
  private @Metric MutableCounterLong numBucketDeleteFails;
  private @Metric MutableCounterLong numKeyAllocateFails;
  private @Metric MutableCounterLong numKeyLookupFails;
  private @Metric MutableCounterLong numKeyRenameFails;
  private @Metric MutableCounterLong numKeyDeleteFails;
  private @Metric MutableCounterLong numBucketListFails;
  private @Metric MutableCounterLong numKeyListFails;
  private @Metric MutableCounterLong numTrashKeyListFails;
  private @Metric MutableCounterLong numVolumeListFails;
  private @Metric MutableCounterLong numKeyCommitFails;
  private @Metric MutableCounterLong numBlockAllocationFails;
  private @Metric MutableCounterLong numGetServiceListFails;
  private @Metric MutableCounterLong numBucketS3ListFails;
  private @Metric MutableCounterLong numInitiateMultipartUploadFails;
  private @Metric MutableCounterLong numCommitMultipartUploadParts;
  private @Metric MutableCounterLong numCommitMultipartUploadPartFails;
  private @Metric MutableCounterLong numCompleteMultipartUploadFails;
  private @Metric MutableCounterLong numAbortMultipartUploads;
  private @Metric MutableCounterLong numAbortMultipartUploadFails;
  private @Metric MutableCounterLong numListMultipartUploadParts;
  private @Metric MutableCounterLong numListMultipartUploadPartFails;
  private @Metric MutableCounterLong numOpenKeyDeleteRequestFails;
  private @Metric MutableCounterLong numDatabaseInfoFails;
  private @Metric MutableCounterLong numDatabaseDeleteFails;
  private @Metric MutableCounterLong numDatabaseCreateFails;
  private @Metric MutableCounterLong numDatabaseListFails;
  private @Metric MutableCounterLong numTableDeleteFails;
  private @Metric MutableCounterLong numTableUpdateFails;
  private @Metric MutableCounterLong numTableCreateFails;
  private @Metric MutableCounterLong numPartitionCreateFails;
  private @Metric MutableCounterLong numDatabaseUpdateFails;
  private @Metric MutableCounterLong numPartitionDeleteFails;
  private @Metric MutableCounterLong numPartitionUpdateFails;
  private @Metric MutableCounterLong numTabletCommitFails;
  private @Metric MutableCounterLong numTabletDeleteFails;
  private @Metric MutableCounterLong numOpenTabletDeleteRequestFails;

  private @Metric MutableCounterLong numGetFileStatusFails;
  private @Metric MutableCounterLong numCreateDirectoryFails;
  private @Metric MutableCounterLong numCreateFileFails;
  private @Metric MutableCounterLong numLookupFileFails;
  private @Metric MutableCounterLong numListStatusFails;

  // Metrics for total number of volumes, buckets and keys

  private @Metric MutableCounterLong numVolumes;
  private @Metric MutableCounterLong numBuckets;
  private @Metric MutableCounterLong numS3Buckets;
  private @Metric MutableCounterLong numDatabases;
  private @Metric MutableCounterLong numTables;
  private @Metric MutableCounterLong numPartitions;

  //TODO: This metric is an estimate and it may be inaccurate on restart if the
  // OM process was not shutdown cleanly. Key creations/deletions in the last
  // few minutes before restart may not be included in this count.
  private @Metric MutableCounterLong numKeys;
  private @Metric MutableCounterLong numTablets;

  private @Metric MutableCounterLong numBucketS3Creates;
  private @Metric MutableCounterLong numBucketS3CreateFails;
  private @Metric MutableCounterLong numBucketS3Deletes;
  private @Metric MutableCounterLong numBucketS3DeleteFails;

  private @Metric MutableCounterLong numListMultipartUploadFails;
  private @Metric MutableCounterLong numListMultipartUploads;

  // Metrics related to OM Trash.
  private @Metric MutableCounterLong numTrashRenames;
  private @Metric MutableCounterLong numTrashDeletes;
  private @Metric MutableCounterLong numTrashListStatus;
  private @Metric MutableCounterLong numTrashGetFileStatus;
  private @Metric MutableCounterLong numTrashGetTrashRoots;
  private @Metric MutableCounterLong numTrashExists;
  private @Metric MutableCounterLong numTrashWriteRequests;
  private @Metric MutableCounterLong numTrashFilesRenames;
  private @Metric MutableCounterLong numTrashFilesDeletes;
  private @Metric MutableCounterLong numTrashActiveCycles;
  private @Metric MutableCounterLong numTrashCheckpointsProcessed;
  private @Metric MutableCounterLong numTrashFails;
  private @Metric MutableCounterLong numTrashRootsEnqueued;
  private @Metric MutableCounterLong numTrashRootsProcessed;

  private final DBCheckpointMetrics dbCheckpointMetrics;

  public OMMetrics() {
    dbCheckpointMetrics = DBCheckpointMetrics.create("OM Metrics");
  }

  public static OMMetrics create() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(SOURCE_NAME,
        "Ozone Manager Metrics",
        new OMMetrics());
  }

  public DBCheckpointMetrics getDBCheckpointMetrics() {
    return dbCheckpointMetrics;
  }

  public void incNumS3BucketCreates() {
    numBucketOps.incr();
    numBucketS3Creates.incr();
  }

  public void incNumS3BucketCreateFails() {
    numBucketS3CreateFails.incr();
  }

  public void incNumS3BucketDeletes() {
    numBucketOps.incr();
    numBucketS3Deletes.incr();
  }

  public void incNumS3BucketDeleteFails() {
    numBucketOps.incr();
    numBucketS3DeleteFails.incr();
  }


  public void incNumS3Buckets() {
    numS3Buckets.incr();
  }

  public void decNumS3Buckets() {
    numS3Buckets.incr();
  }

  public void incNumVolumes() {
    numVolumes.incr();
  }

  public void incNumDatabases() {
    numDatabases.incr();
  }

  public void decNumVolumes() {
    numVolumes.incr(-1);
  }

  public void incNumBuckets() {
    numBuckets.incr();
  }

  public void decNumBuckets() {
    numBuckets.incr(-1);
  }

  public void incNumKeys() {
    numKeys.incr();
  }

  public void incNumKeys(int count) {
    numKeys.incr(count);
  }

  public void decNumKeys() {
    numKeys.incr(-1);
  }

  public void incNumTablets() {
    numTablets.incr();
  }

  public void incNumTablets(int count) {
    numTablets.incr(count);
  }

  public void decNumTablets() {
    numTablets.incr(-1);
  }

  public void setNumDatabases(long val) {
    long oldVal = this.numDatabases.value();
    this.numDatabases.incr(val - oldVal);
  }

  public void setNumVolumes(long val) {
    long oldVal = this.numVolumes.value();
    this.numVolumes.incr(val - oldVal);
  }

  public void setNumBuckets(long val) {
    long oldVal = this.numBuckets.value();
    this.numBuckets.incr(val - oldVal);
  }

  public void setNumTables(long val) {
    long oldVal = this.numTables.value();
    this.numTables.incr(val - oldVal);
  }

  public void setNumPartitions(long val) {
    long oldVal = this.numPartitions.value();
    this.numPartitions.incr(val - oldVal);
  }

  public void setNumKeys(long val) {
    long oldVal = this.numKeys.value();
    this.numKeys.incr(val- oldVal);
  }

  public void decNumKeys(long val) {
    this.numKeys.incr(-val);
  }

  public void decNumTablets(long val) {
    this.numTablets.incr(-val);
  }

  public long getNumVolumes() {
    return numVolumes.value();
  }

  public long getNumBuckets() {
    return numBuckets.value();
  }

  public long getNumTables() {
    return numTables.value();
  }

  public long getNumPartitions() {
    return numPartitions.value();
  }

  public long getNumKeys() {
    return numKeys.value();
  }


  public void incNumVolumeCreates() {
    numVolumeOps.incr();
    numVolumeCreates.incr();
  }

  public void incNumVolumeUpdates() {
    numVolumeOps.incr();
    numVolumeUpdates.incr();
  }

  public void incNumVolumeInfos() {
    numVolumeOps.incr();
    numVolumeInfos.incr();
  }

  public void incNumVolumeDeletes() {
    numVolumeOps.incr();
    numVolumeDeletes.incr();
  }

  public void incNumVolumeCheckAccesses() {
    numVolumeOps.incr();
    numVolumeCheckAccesses.incr();
  }

  public void incNumBucketCreates() {
    numBucketOps.incr();
    numBucketCreates.incr();
  }

  public void incNumBucketInfos() {
    numBucketOps.incr();
    numBucketInfos.incr();
  }

  public void incNumBucketUpdates() {
    numBucketOps.incr();
    numBucketUpdates.incr();
  }

  public void incNumBucketDeletes() {
    numBucketOps.incr();
    numBucketDeletes.incr();
  }

  public void incNumBucketLists() {
    numBucketOps.incr();
    numBucketLists.incr();
  }

  public void incNumKeyLists() {
    numKeyOps.incr();
    numKeyLists.incr();
  }

  public void incNumTrashKeyLists() {
    numKeyOps.incr();
    numTrashKeyLists.incr();
  }

  public void incNumVolumeLists() {
    numVolumeOps.incr();
    numVolumeLists.incr();
  }

  public void incNumListS3Buckets() {
    numBucketOps.incr();
    numBucketS3Lists.incr();
  }

  public void incNumListS3BucketsFails() {
    numBucketOps.incr();
    numBucketS3ListFails.incr();
  }

  public void incNumInitiateMultipartUploads() {
    numKeyOps.incr();
    numInitiateMultipartUploads.incr();
  }

  public void incNumInitiateMultipartUploadFails() {
    numInitiateMultipartUploadFails.incr();
  }

  public void incNumCommitMultipartUploadParts() {
    numKeyOps.incr();
    numCommitMultipartUploadParts.incr();
  }

  public void incNumCommitMultipartUploadPartFails() {
    numCommitMultipartUploadPartFails.incr();
  }

  public void incNumCompleteMultipartUploads() {
    numKeyOps.incr();
    numCompleteMultipartUploads.incr();
  }

  public void incNumCompleteMultipartUploadFails() {
    numCompleteMultipartUploadFails.incr();
  }

  public void incNumAbortMultipartUploads() {
    numKeyOps.incr();
    numAbortMultipartUploads.incr();
  }

  public void incNumListMultipartUploadFails() {
    numListMultipartUploadFails.incr();
  }

  public void incNumListMultipartUploads() {
    numKeyOps.incr();
    numListMultipartUploads.incr();
  }

  public void incNumAbortMultipartUploadFails() {
    numAbortMultipartUploadFails.incr();
  }
  public void incNumListMultipartUploadParts() {
    numKeyOps.incr();
    numListMultipartUploadParts.incr();
  }

  public void incNumGetFileStatus() {
    numKeyOps.incr();
    numFSOps.incr();
    numGetFileStatus.incr();
  }

  public void incNumGetFileStatusFails() {
    numGetFileStatusFails.incr();
  }

  public void incNumCreateDirectory() {
    numKeyOps.incr();
    numFSOps.incr();
    numCreateDirectory.incr();
  }

  public void incNumCreateDirectoryFails() {
    numCreateDirectoryFails.incr();
  }

  public void incNumCreateFile() {
    numKeyOps.incr();
    numFSOps.incr();
    numCreateFile.incr();
  }

  public void incNumCreateFileFails() {
    numCreateFileFails.incr();
  }

  public void incNumLookupFile() {
    numKeyOps.incr();
    numFSOps.incr();
    numLookupFile.incr();
  }

  public void incNumLookupFileFails() {
    numLookupFileFails.incr();
  }

  public void incNumListStatus() {
    numKeyOps.incr();
    numFSOps.incr();
    numListStatus.incr();
  }

  public void incNumListStatusFails() {
    numListStatusFails.incr();
  }

  public void incNumListMultipartUploadPartFails() {
    numListMultipartUploadPartFails.incr();
  }

  public void incNumGetServiceLists() {
    numGetServiceLists.incr();
  }

  public void incNumVolumeCreateFails() {
    numVolumeCreateFails.incr();
  }

  public void incNumDatabaseCreateFails() {
    numDatabaseCreateFails.incr();
  }

  public void incNumVolumeUpdateFails() {
    numVolumeUpdateFails.incr();
  }

  public void incNumVolumeInfoFails() {
    numVolumeInfoFails.incr();
  }

  public void incNumVolumeDeleteFails() {
    numVolumeDeleteFails.incr();
  }

  public void incNumVolumeCheckAccessFails() {
    numVolumeCheckAccessFails.incr();
  }

  public void incNumBucketCreateFails() {
    numBucketCreateFails.incr();
  }

  public void incNumTableCreateFails() {
    numTableCreateFails.incr();
  }

  public void incNumPartitionCreateFails() {
    numPartitionCreateFails.incr();
  }

  public void incNumBucketInfoFails() {
    numBucketInfoFails.incr();
  }

  public void incNumBucketUpdateFails() {
    numBucketUpdateFails.incr();
  }

  public void incNumBucketDeleteFails() {
    numBucketDeleteFails.incr();
  }

  public void incNumKeyAllocates() {
    numKeyOps.incr();
    numKeyAllocate.incr();
  }

  public void incNumKeyAllocateFails() {
    numKeyAllocateFails.incr();
  }

  public void incNumKeyLookups() {
    numKeyOps.incr();
    numKeyLookup.incr();
  }

  public void incNumKeyLookupFails() {
    numKeyLookupFails.incr();
  }

  public void incNumKeyRenames() {
    numKeyOps.incr();
    numKeyRenames.incr();
  }

  public void incNumKeyRenameFails() {
    numKeyOps.incr();
    numKeyRenameFails.incr();
  }

  public void incNumKeyDeleteFails() {
    numKeyDeleteFails.incr();
  }

  public void incNumTabletDeleteFails() {
    numTabletDeleteFails.incr();
  }

  public void incNumKeyDeletes() {
    numKeyOps.incr();
    numKeyDeletes.incr();
  }

  public void incNumTabletDeletes() {
    numTabletOps.incr();
    numTabletDeletes.incr();
  }

  public void incNumKeyCommits() {
    numKeyOps.incr();
    numKeyCommits.incr();
  }

  public void incNumTabletCommits() {
    numTabletOps.incr();
    numTabletCommits.incr();
  }

  public void incNumKeyCommitFails() {
    numKeyCommitFails.incr();
  }


  public void incNumTabletCommitFails() {
    numTabletCommitFails.incr();
  }

  public void incNumBlockAllocateCalls() {
    numBlockAllocations.incr();
  }

  public void incNumBlockAllocateCallFails() {
    numBlockAllocationFails.incr();
  }

  public void incNumBucketListFails() {
    numBucketListFails.incr();
  }

  public void incNumKeyListFails() {
    numKeyListFails.incr();
  }

  public void incNumTrashKeyListFails() {
    numTrashKeyListFails.incr();
  }

  public void incNumVolumeListFails() {
    numVolumeListFails.incr();
  }

  public void incNumGetServiceListFails() {
    numGetServiceListFails.incr();
  }

  public void incNumOpenKeyDeleteRequests() {
    numOpenKeyDeleteRequests.incr();
  }

  public void incNumOpenTabletDeleteRequests() {
    numOpenTabletDeleteRequests.incr();
  }

  public void incNumOpenKeysSubmittedForDeletion(long amount) {
    numOpenKeysSubmittedForDeletion.incr(amount);
  }

  public void incNumOpenTabletsSubmittedForDeletion(long amount) {
    numOpenTabletsSubmittedForDeletion.incr(amount);
  }

  public void incNumOpenKeysDeleted() {
    numOpenKeysDeleted.incr();
  }

  public void incNumOpenTabletsDeleted() {
    numOpenTabletsDeleted.incr();
  }

  public void incNumOpenKeyDeleteRequestFails() {
    numOpenKeyDeleteRequestFails.incr();
  }

  public void incNumOpenTabletDeleteRequestFails() {
    numOpenTabletDeleteRequestFails.incr();
  }

  public void incNumAddAcl() {
    numAddAcl.incr();
  }

  public void incNumSetAcl() {
    numSetAcl.incr();
  }

  public void incNumGetAcl() {
    numGetAcl.incr();
  }

  public void incNumRemoveAcl() {
    numRemoveAcl.incr();
  }

  @VisibleForTesting
  public long getNumVolumeCreates() {
    return numVolumeCreates.value();
  }

  @VisibleForTesting
  public long getNumDatabaseCreates() {
    return numDatabaseCreates.value();
  }

  @VisibleForTesting
  public long getNumVolumeUpdates() {
    return numVolumeUpdates.value();
  }

  @VisibleForTesting
  public long getNumVolumeInfos() {
    return numVolumeInfos.value();
  }

  @VisibleForTesting
  public long getNumVolumeDeletes() {
    return numVolumeDeletes.value();
  }

  @VisibleForTesting
  public long getNumDatabaseDeletes() {
    return numDatabaseDeletes.value();
  }

  @VisibleForTesting
  public long getNumVolumeCheckAccesses() {
    return numVolumeCheckAccesses.value();
  }

  @VisibleForTesting
  public long getNumBucketCreates() {
    return numBucketCreates.value();
  }

  @VisibleForTesting
  public long getNumTableCreates() {
    return numTableCreates.value();
  }

  @VisibleForTesting
  public long getNumPartitionCreates() {
    return numPartitionCreates.value();
  }


  @VisibleForTesting
  public long getNumBucketInfos() {
    return numBucketInfos.value();
  }

  @VisibleForTesting
  public long getNumBucketUpdates() {
    return numBucketUpdates.value();
  }

  @VisibleForTesting
  public long getNumBucketDeletes() {
    return numBucketDeletes.value();
  }

  @VisibleForTesting
  public long getNumTableDeletes() {
    return numTableDeletes.value();
  }

  @VisibleForTesting
  public long getNumPartitionDeletes() {
    return numPartitionDeletes.value();
  }

  @VisibleForTesting
  public long getNumTableUpdates() {
    return numTableUpdates.value();
  }

  @VisibleForTesting
  public long getNumPartitionUpdates() {
    return numPartitionUpdates.value();
  }

  @VisibleForTesting
  public long getNumDatabaseUpdates() {
    return numDatabaseUpdates.value();
  }

  @VisibleForTesting
  public long getNumBucketLists() {
    return numBucketLists.value();
  }

  @VisibleForTesting
  public long getNumVolumeLists() {
    return numVolumeLists.value();
  }

  @VisibleForTesting
  public long getNumKeyLists() {
    return numKeyLists.value();
  }

  @VisibleForTesting
  public long getNumTrashKeyLists() {
    return numTrashKeyLists.value();
  }

  @VisibleForTesting
  public long getNumGetServiceLists() {
    return numGetServiceLists.value();
  }

  @VisibleForTesting
  public long getNumVolumeCreateFails() {
    return numVolumeCreateFails.value();
  }

  @VisibleForTesting
  public long getNumVolumeUpdateFails() {
    return numVolumeUpdateFails.value();
  }

  @VisibleForTesting
  public long getNumVolumeInfoFails() {
    return numVolumeInfoFails.value();
  }

  @VisibleForTesting
  public long getNumVolumeDeleteFails() {
    return numVolumeDeleteFails.value();
  }

  @VisibleForTesting
  public long getNumDatabaseDeleteFails() {
    return numDatabaseDeleteFails.value();
  }

  @VisibleForTesting
  public long getNumVolumeCheckAccessFails() {
    return numVolumeCheckAccessFails.value();
  }

  @VisibleForTesting
  public long getNumBucketCreateFails() {
    return numBucketCreateFails.value();
  }

  @VisibleForTesting
  public long getNumBucketInfoFails() {
    return numBucketInfoFails.value();
  }

  @VisibleForTesting
  public long getNumBucketUpdateFails() {
    return numBucketUpdateFails.value();
  }

  @VisibleForTesting
  public long getNumBucketDeleteFails() {
    return numBucketDeleteFails.value();
  }

  @VisibleForTesting
  public long getNumTableDeleteFails() {
    return numTableDeleteFails.value();
  }

  @VisibleForTesting
  public long getNumPartitionDeleteFails() {
    return numPartitionDeleteFails.value();
  }

  @VisibleForTesting
  public long getNumTableUpdateFails() {
    return numTableUpdateFails.value();
  }

  @VisibleForTesting
  public long getNumPartitionUpdateFails() {
    return numPartitionUpdateFails.value();
  }

  @VisibleForTesting
  public long getNumDatabaseUpdateFails() {
    return numDatabaseUpdateFails.value();
  }

  @VisibleForTesting
  public long getNumKeyAllocates() {
    return numKeyAllocate.value();
  }

  @VisibleForTesting
  public long getNumKeyAllocateFails() {
    return numKeyAllocateFails.value();
  }

  @VisibleForTesting
  public long getNumKeyLookups() {
    return numKeyLookup.value();
  }

  @VisibleForTesting
  public long getNumKeyLookupFails() {
    return numKeyLookupFails.value();
  }

  @VisibleForTesting
  public long getNumKeyRenames() {
    return numKeyRenames.value();
  }

  @VisibleForTesting
  public long getNumKeyRenameFails() {
    return numKeyRenameFails.value();
  }

  @VisibleForTesting
  public long getNumKeyDeletes() {
    return numKeyDeletes.value();
  }

  @VisibleForTesting
  public long getNumKeyDeletesFails() {
    return numKeyDeleteFails.value();
  }

  @VisibleForTesting
  public long getNumTabletDeletesFails() {
    return numTabletDeleteFails.value();
  }

  @VisibleForTesting
  public long getNumBucketListFails() {
    return numBucketListFails.value();
  }

  @VisibleForTesting
  public long getNumKeyListFails() {
    return numKeyListFails.value();
  }

  @VisibleForTesting
  public long getNumTrashKeyListFails() {
    return numTrashKeyListFails.value();
  }

  @VisibleForTesting
  public long getNumFSOps() {
    return numFSOps.value();
  }

  @VisibleForTesting
  public long getNumGetFileStatus() {
    return numGetFileStatus.value();
  }

  @VisibleForTesting
  public long getNumListStatus() {
    return numListStatus.value();
  }

  @VisibleForTesting
  public long getNumVolumeListFails() {
    return numVolumeListFails.value();
  }

  @VisibleForTesting
  public long getNumKeyCommits() {
    return numKeyCommits.value();
  }

  @VisibleForTesting
  public long getNumTabletCommits() {
    return numTabletCommits.value();
  }

  @VisibleForTesting
  public long getNumKeyCommitFails() {
    return numKeyCommitFails.value();
  }

  @VisibleForTesting
  public long getNumBlockAllocates() {
    return numBlockAllocations.value();
  }

  @VisibleForTesting
  public long getNumBlockAllocateFails() {
    return numBlockAllocationFails.value();
  }

  @VisibleForTesting
  public long getNumGetServiceListFails() {
    return numGetServiceListFails.value();
  }

  @VisibleForTesting
  public long getNumListS3Buckets() {
    return numBucketS3Lists.value();
  }

  @VisibleForTesting
  public long getNumListS3BucketsFails() {
    return numBucketS3ListFails.value();
  }

  public long getNumInitiateMultipartUploads() {
    return numInitiateMultipartUploads.value();
  }

  public long getNumInitiateMultipartUploadFails() {
    return numInitiateMultipartUploadFails.value();
  }

  public long getNumAbortMultipartUploads() {
    return numAbortMultipartUploads.value();
  }

  public long getNumAbortMultipartUploadFails() {
    return numAbortMultipartUploadFails.value();
  }

  public long getNumOpenKeyDeleteRequests() {
    return numOpenKeyDeleteRequests.value();
  }

  public long getNumOpenTabletDeleteRequests() {
    return numOpenTabletDeleteRequests.value();
  }

  public long getNumOpenKeysSubmittedForDeletion() {
    return numOpenKeysSubmittedForDeletion.value();
  }

  public long getNumOpenTabletsSubmittedForDeletion() {
    return numOpenTabletsSubmittedForDeletion.value();
  }

  public long getNumOpenKeysDeleted() {
    return numOpenKeysDeleted.value();
  }

  public long getNumOpenTabletsDeleted() {
    return numOpenTabletsDeleted.value();
  }

  public long getNumOpenKeyDeleteRequestFails() {
    return numOpenKeyDeleteRequestFails.value();
  }

  public long getNumOpenTabletDeleteRequestFails() {
    return numOpenTabletDeleteRequestFails.value();
  }

  public long getNumAddAcl() {
    return numAddAcl.value();
  }

  public long getNumSetAcl() {
    return numSetAcl.value();
  }

  public long getNumGetAcl() {
    return numGetAcl.value();
  }

  public long getNumRemoveAcl() {
    return numRemoveAcl.value();
  }

  public void incNumTrashRenames() {
    numTrashRenames.incr();
  }

  public long getNumTrashRenames() {
    return numTrashRenames.value();
  }

  public void incNumTrashDeletes() {
    numTrashDeletes.incr();
  }

  public long getNumTrashDeletes() {
    return numTrashDeletes.value();
  }

  public void incNumTrashListStatus() {
    numTrashListStatus.incr();
  }

  public void incNumTrashGetFileStatus() {
    numTrashGetFileStatus.incr();
  }

  public void incNumTrashGetTrashRoots() {
    numTrashGetTrashRoots.incr();
  }

  public void incNumTrashExists() {
    numTrashExists.incr();
  }

  public void incNumTrashWriteRequests() {
    numTrashWriteRequests.incr();
  }

  public void incNumTrashFilesRenames() {
    numTrashFilesRenames.incr();
  }

  public long getNumTrashFilesRenames() {
    return numTrashFilesRenames.value();
  }

  public void incNumTrashFilesDeletes() {
    numTrashFilesDeletes.incr();
  }

  public long getNumTrashFilesDeletes() {
    return numTrashFilesDeletes.value();
  }


  public void incNumTrashActiveCycles() {
    numTrashActiveCycles.incr();
  }

  public void incNumTrashRootsEnqueued() {
    numTrashRootsEnqueued.incr();
  }

  public void incNumTrashRootsProcessed() {
    numTrashRootsProcessed.incr();
  }

  public void incNumTrashFails() {
    numTrashFails.incr();
  }

  public void unRegister() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE_NAME);
  }

  public void incNumDatabaseInfos() {
    numDatabaseOps.incr();
    numDatabaseInfos.incr();
  }

  public void incNumDatabaseInfoFails() {
    numDatabaseInfoFails.incr();
  }

  public void incNumDatabaseLists() {
    numDatabaseOps.incr();
    numDatabaseLists.incr();
  }

  public void incNumDatabaseListFails() {
    numDatabaseListFails.incr();
  }

  public void incNumDatabaseCreates() {
    numDatabaseOps.incr();
    numDatabaseCreates.incr();
  }

  public void incNumDatabaseDeletes() {
    numDatabaseOps.incr();
    numDatabaseDeletes.incr();
  }

  public void decNumDatabases() {
    numDatabases.incr(-1);
  }

  public void incNumDatabaseDeleteFails() {
    numDatabaseDeleteFails.incr();
  }

  public void incNumTableCreates() {
    numTableOps.incr();
    numTableCreates.incr();
  }

  public void incNumPartitionCreates() {
    numPartitionOps.incr();
    numPartitionCreates.incr();
  }

  public void incNumTables() {
    numTables.incr();
  }

  public void incNumPartitions() {
    numPartitions.incr();
  }

  public void incNumTableDeletes() {
    numTableOps.incr();
    numTableDeletes.incr();
  }

  public void incNumPartitionDeletes() {
    numPartitionOps.incr();
    numPartitionDeletes.incr();
  }

  public void incNumTableDeleteFails() {
    numTableDeleteFails.incr();
  }

  public void incNumPartitionDeleteFails() {
    numPartitionDeleteFails.incr();
  }

  public void decNumTables() {
    numTables.incr(-1);
  }

  public void decNumPartitions() {
    numPartitions.incr(-1);
  }

  public void incNumTableUpdates() {
    numTableOps.incr();
    numTableUpdates.incr();
  }

  public void incNumPartitionUpdates() {
    numPartitionOps.incr();
    numPartitionUpdates.incr();
  }

  public void incNumTableUpdateFails() {
    numTableUpdateFails.incr();
  }

  public void incNumPartitionUpdateFails() {
    numPartitionUpdateFails.incr();
  }

  public void incNumDatabaseUpdates() {
    numDatabaseOps.incr();
    numDatabaseUpdates.incr();
  }

  public void incNumDatabaseUpdateFails() {
    numDatabaseUpdateFails.incr();
  }

  public void incNumTabletAllocates() {
    numTabletOps.incr();
    numTabletAllocate.incr();
  }
}
