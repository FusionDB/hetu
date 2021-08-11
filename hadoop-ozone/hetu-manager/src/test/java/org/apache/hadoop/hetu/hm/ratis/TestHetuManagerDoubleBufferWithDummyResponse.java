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

package org.apache.hadoop.hetu.hm.ratis;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hetu.om.OmMetadataManagerImpl;
import org.apache.hadoop.hetu.om.ratis.OzoneManagerDoubleBuffer;
import org.apache.hadoop.hetu.om.ratis.OzoneManagerRatisSnapshot;
import org.apache.hadoop.hetu.om.ratis.metrics.OzoneManagerDoubleBufferMetrics;
import org.apache.hadoop.hetu.om.response.CleanupTableInfo;
import org.apache.hadoop.hetu.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmTableInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateTableResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.hetu.om.OmMetadataManagerImpl.META_TABLE;
import static org.apache.hadoop.hetu.om.request.TestOMRequestUtils.getOmTableInfo;
import static org.apache.hadoop.ozone.OzoneConsts.TRANSACTION_INFO_KEY;
import static org.apache.ozone.test.GenericTestUtils.waitFor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * This class tests OzoneManagerDoubleBuffer implementation with
 * dummy response class.
 */
public class TestHetuManagerDoubleBufferWithDummyResponse {

  private OMMetadataManager omMetadataManager;
  private OzoneManagerDoubleBuffer doubleBuffer;
  private final AtomicLong trxId = new AtomicLong(0);
  private long lastAppliedIndex;
  private long term = 1L;

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Before
  public void setup() throws IOException {
    OzoneConfiguration configuration = new OzoneConfiguration();
    configuration.set(OZONE_METADATA_DIRS,
        folder.newFolder().getAbsolutePath());
    omMetadataManager =
        new OmMetadataManagerImpl(configuration);
    OzoneManagerRatisSnapshot ozoneManagerRatisSnapshot = index -> {
      lastAppliedIndex = index.get(index.size() - 1);
    };
    doubleBuffer = new OzoneManagerDoubleBuffer.Builder()
        .setOmMetadataManager(omMetadataManager)
        .setOzoneManagerRatisSnapShot(ozoneManagerRatisSnapshot)
        .enableRatis(true)
        .setIndexToTerm((val) -> term)
        .build();
  }

  @After
  public void stop() {
    doubleBuffer.stop();
  }

  /**
   * This tests add's 100 table creation responses to doubleBuffer, and
   * check OM DB bucket table has 100 entries or not. In addition checks
   * flushed transaction count is matching with expected count or not.
   */
  @Test(timeout = 300_000)
  public void testDoubleBufferWithDummyResponse() throws Exception {
    String databaseName = UUID.randomUUID().toString();
    String tablePrefix = "table";
    int tableCount = 100;
    OzoneManagerDoubleBufferMetrics metrics =
        doubleBuffer.getOzoneManagerDoubleBufferMetrics();

    // As we have not flushed/added any transactions, all metrics should have
    // value zero.
    assertEquals(0, metrics.getTotalNumOfFlushOperations());
    assertEquals(0, metrics.getTotalNumOfFlushedTransactions());
    assertEquals(0, metrics.getMaxNumberOfTransactionsFlushedInOneIteration());

    for (int i=0; i < tableCount; i++) {
      doubleBuffer.add(createDummyTableResponse(databaseName, tablePrefix + "-" + i),
          trxId.incrementAndGet());
    }
    waitFor(() -> metrics.getTotalNumOfFlushedTransactions() == tableCount,
        100, 60000);

    assertTrue(metrics.getTotalNumOfFlushOperations() > 0);
    assertEquals(tableCount, doubleBuffer.getFlushedTransactionCount());
    assertTrue(metrics.getMaxNumberOfTransactionsFlushedInOneIteration() > 0);
    assertEquals(tableCount, omMetadataManager.countRowsInTable(
        omMetadataManager.getMetaTable()));
    assertTrue(doubleBuffer.getFlushIterations() > 0);
    assertTrue(metrics.getFlushTime().lastStat().numSamples() > 0);
    assertTrue(metrics.getAvgFlushTransactionsInOneIteration() > 0);

    // Assert there is only instance of OM Double Metrics.
    OzoneManagerDoubleBufferMetrics metricsCopy =
        OzoneManagerDoubleBufferMetrics.create();
    assertEquals(metrics, metricsCopy);

    // Check lastAppliedIndex is updated correctly or not.
    assertEquals(tableCount, lastAppliedIndex);


    TransactionInfo transactionInfo =
        omMetadataManager.getTransactionInfoTable().get(TRANSACTION_INFO_KEY);
    assertNotNull(transactionInfo);

    Assert.assertEquals(lastAppliedIndex,
        transactionInfo.getTransactionIndex());
    Assert.assertEquals(term, transactionInfo.getTerm());
  }

  /**
   * Create DummyTableCreate response.
   */
  private OMDummyCreateTableResponse createDummyTableResponse(
      String databaseName, String tableName) {
    OmTableInfo omTableInfo = getOmTableInfo(databaseName, tableName);
    return new OMDummyCreateTableResponse(omTableInfo,
        OMResponse.newBuilder()
            .setCmdType(OzoneManagerProtocolProtos.Type.CreateTable)
            .setStatus(OzoneManagerProtocolProtos.Status.OK)
            .setCreateTableResponse(CreateTableResponse.newBuilder().build())
            .build());
  }


  /**
   * DummyCreatedTable Response class used in testing.
   */
  @CleanupTableInfo(cleanupTables = {META_TABLE})
  private static class OMDummyCreateTableResponse extends OMClientResponse {
    private final OmTableInfo omTableInfo;

    OMDummyCreateTableResponse(OmTableInfo omTableInfo,
        OMResponse omResponse) {
      super(omResponse);
      this.omTableInfo = omTableInfo;
    }

    @Override
    public void addToDBBatch(OMMetadataManager omMetadataManager,
        BatchOperation batchOperation) throws IOException {
      String dbTableKey =
          omMetadataManager.getMetaTableKey(omTableInfo.getDatabaseName(),
                  omTableInfo.getTableName());
      omMetadataManager.getMetaTable().putWithBatch(batchOperation,
          dbTableKey, omTableInfo);
    }
  }
}
