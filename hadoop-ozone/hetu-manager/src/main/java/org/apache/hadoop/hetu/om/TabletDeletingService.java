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
package org.apache.hadoop.hetu.om;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ServiceException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult.EmptyTaskResult;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.common.DeleteBlockGroupResult;
import org.apache.hadoop.ozone.om.helpers.OMRatisHelper;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeletedTablets;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PurgeTabletsRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.util.Time;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.util.Preconditions;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.hadoop.ozone.OzoneConsts.OM_TABLET_PREFIX;
import static org.apache.hadoop.ozone.om.OMConfigKeys.*;

/**
 * This is the background service to delete tablets. Scan the metadata of om
 * periodically to get the tablets from DeletedTable and ask scm to delete
 * metadata accordingly, if scm returns success for tablets, then clean up those
 * tablets.
 */
public class TabletDeletingService extends BackgroundService {
  private static final Logger LOG =
      LoggerFactory.getLogger(TabletDeletingService.class);

  // Use only a single thread for TabletDeletion. Multiple threads would read
  // from the same table and can send deletion requests for same tablet multiple
  // times.
  private static final int TABLET_DELETING_CORE_POOL_SIZE = 1;

  private final OzoneManager ozoneManager;
  private final ScmBlockLocationProtocol scmClient;
  private final TabletManager manager;
  private static ClientId clientId = ClientId.randomId();
  private final int tabletLimitPerTask;
  private final AtomicLong deletedTabletCount;
  private final AtomicLong runCount;

  TabletDeletingService(OzoneManager ozoneManager,
                        ScmBlockLocationProtocol scmClient,
                        TabletManager manager, long serviceInterval,
                        long serviceTimeout, ConfigurationSource conf) {
    super("TabletDeletingService", serviceInterval, TimeUnit.MILLISECONDS,
        TABLET_DELETING_CORE_POOL_SIZE, serviceTimeout);
    this.ozoneManager = ozoneManager;
    this.scmClient = scmClient;
    this.manager = manager;
    this.tabletLimitPerTask = conf.getInt(OZONE_TABLET_DELETING_LIMIT_PER_TASK,
        OZONE_TABLET_DELETING_LIMIT_PER_TASK_DEFAULT);
    this.deletedTabletCount = new AtomicLong(0);
    this.runCount = new AtomicLong(0);
  }

  /**
   * Returns the number of times this Background service has run.
   *
   * @return Long, run count.
   */
  @VisibleForTesting
  public AtomicLong getRunCount() {
    return runCount;
  }

  /**
   * Returns the number of tablets deleted by the background service.
   *
   * @return Long count.
   */
  @VisibleForTesting
  public AtomicLong getDeletedTabletCount() {
    return deletedTabletCount;
  }

  @Override
  public BackgroundTaskQueue getTasks() {
    BackgroundTaskQueue queue = new BackgroundTaskQueue();
    queue.add(new TabletDeletingTask());
    return queue;
  }

  private boolean shouldRun() {
    if (ozoneManager == null) {
      // OzoneManager can be null for testing
      return true;
    }
    return ozoneManager.isLeaderReady();
  }

  private boolean isRatisEnabled() {
    if (ozoneManager == null) {
      return false;
    }
    return ozoneManager.isRatisEnabled();
  }

  /**
   * A tablet deleting task scans OM DB and looking for a certain number of
   * pending-deletion tablets, sends these tablets along with their associated blocks
   * to SCM for deletion. Once SCM confirms tablets are deleted (once SCM persisted
   * the blocks info in its deletedBlockLog), it removes these tablets from the
   * DB.
   */
  private class TabletDeletingTask implements BackgroundTask {

    @Override
    public int getPriority() {
      return 0;
    }

    @Override
    public BackgroundTaskResult call() throws Exception {
      // Check if this is the Leader OM. If not leader, no need to execute this
      // task.
      if (shouldRun()) {
        runCount.incrementAndGet();
        try {
          long startTime = Time.monotonicNow();
          List<BlockGroup> tabletBlocksList = manager
              .getPendingDeletionTablets(tabletLimitPerTask);
          if (tabletBlocksList != null && !tabletBlocksList.isEmpty()) {
            List<DeleteBlockGroupResult> results =
                scmClient.deleteTabletBlocks(tabletBlocksList);
            if (results != null) {
              int delCount;
              if (isRatisEnabled()) {
                delCount = submitPurgeTabletsRequest(results);
              } else {
                // TODO: Once HA and non-HA paths are merged, we should have
                //  only one code path here. Purge tablets should go through an
                //  OMRequest model.
                delCount = deleteAllTablets(results);
              }
              LOG.debug("Number of tablets deleted: {}, elapsed time: {}ms",
                  delCount, Time.monotonicNow() - startTime);
              deletedTabletCount.addAndGet(delCount);
            }
          }
        } catch (IOException e) {
          LOG.error("Error while running delete tablets background task. Will " +
              "retry at next run.", e);
        }
      }
      // By design, no one cares about the results of this call back.
      return EmptyTaskResult.newResult();
    }

    /**
     * Deletes all the tablets that SCM has acknowledged and queued for delete.
     *
     * @param results DeleteBlockGroups returned by SCM.
     * @throws RocksDBException on Error.
     * @throws IOException      on Error
     */
    private int deleteAllTablets(List<DeleteBlockGroupResult> results)
        throws RocksDBException, IOException {
      Table deletedTable = manager.getMetadataManager().getDeletedTablet();

      DBStore store = manager.getMetadataManager().getStore();

      // Put all tablets to delete in a single transaction and call for delete.
      int deletedCount = 0;
      try (BatchOperation writeBatch = store.initBatchOperation()) {
        for (DeleteBlockGroupResult result : results) {
          if (result.isSuccess()) {
            // Purge tablet from OM DB.
            deletedTable.deleteWithBatch(writeBatch,
                result.getObjectKey());
            LOG.debug("Tablet {} deleted from OM DB", result.getObjectKey());
            deletedCount++;
          }
        }
        // Write a single transaction for delete.
        store.commitBatchOperation(writeBatch);
      }
      return deletedCount;
    }

    /**
     * Submits PurgeTablets request for the tablets whose blocks have been deleted
     * by SCM.
     *
     * @param results DeleteBlockGroups returned by SCM.
     * @throws IOException      on Error
     */
    public int submitPurgeTabletsRequest(List<DeleteBlockGroupResult> results) {
      Map<Pair<String, String>, List<String>> purgeTabletsMapPerPartition =
          new HashMap<>();

      // Put all tablets to be purged in a list
      int deletedCount = 0;
      for (DeleteBlockGroupResult result : results) {
        if (result.isSuccess()) {
          // Add tablet to PurgeTablets list.
          String deletedTablet = result.getObjectKey();
          // Parse Database, tableName and partitionName
          addToMap(purgeTabletsMapPerPartition, deletedTablet);
          LOG.debug("Tablet {} set to be purged from OM DB", deletedTablet);
          deletedCount++;
        }
      }

      PurgeTabletsRequest.Builder purgeTabletsRequest = PurgeTabletsRequest.newBuilder();

      // Add tablets to PurgeTabletsRequest partition wise.
      for (Map.Entry<Pair<String, String>, List<String>> entry :
          purgeTabletsMapPerPartition.entrySet()) {
        Pair<String, String> databaseTablePartitionPair = entry.getKey();
        String[] tablePartitionRight = databaseTablePartitionPair.getRight().split(OM_TABLET_PREFIX);
        DeletedTablets deletedTabletsInPartition = DeletedTablets.newBuilder()
            .setDatabaseName(databaseTablePartitionPair.getLeft())
            .setTableName(tablePartitionRight[0])
            .setPartitionName(tablePartitionRight[1])
            .addAllTablets(entry.getValue())
            .build();
        purgeTabletsRequest.addDeletedTablets(deletedTabletsInPartition);
      }

      OMRequest omRequest = OMRequest.newBuilder()
          .setCmdType(Type.PurgeTablets)
          .setPurgeTabletsRequest(purgeTabletsRequest)
          .setClientId(clientId.toString())
          .build();

      // Submit PurgeTablets request to OM
      try {
        RaftClientRequest raftClientRequest =
            createRaftClientRequestForPurge(omRequest);
        ozoneManager.getOmRatisServer().submitRequest(omRequest,
            raftClientRequest);
      } catch (ServiceException e) {
        LOG.error("PurgeTablet request failed. Will retry at next run.");
        return 0;
      }

      return deletedCount;
    }
  }

  private RaftClientRequest createRaftClientRequestForPurge(
      OMRequest omRequest) {
    return RaftClientRequest.newBuilder()
        .setClientId(clientId)
        .setServerId(ozoneManager.getOmRatisServer().getRaftPeerId())
        .setGroupId(ozoneManager.getOmRatisServer().getRaftGroupId())
        .setCallId(runCount.get())
        .setMessage(
            Message.valueOf(
                OMRatisHelper.convertRequestToByteString(omRequest)))
        .setType(RaftClientRequest.writeRequestType())
        .build();
  }

  /**
   * Parse Database, Table and Partition Name from ObjectTablet and add it to given map of
   * tablets to be purged per partition.
   */
  private void addToMap(Map<Pair<String, String>, List<String>> map,
      String objectTablet) {
    // Parse database, table and partition name
    String[] split = objectTablet.split(OM_TABLET_PREFIX);
    Preconditions.assertTrue(split.length > 4, "Database, Table and/or Partition Name " +
        "missing from tablet Name.");
    Pair<String, String> databaseTablePartitionPair = Pair.of(split[1], split[2] + OM_TABLET_PREFIX + split[3]);
    if (!map.containsKey(databaseTablePartitionPair)) {
      map.put(databaseTablePartitionPair, new ArrayList<>());
    }
    map.get(databaseTablePartitionPair).add(objectTablet);
  }
}
