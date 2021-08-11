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
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hetu.hm.helpers.OmDatabaseArgs;
import org.apache.hadoop.hetu.om.OMMetrics;
import org.apache.hadoop.hetu.om.OmMetadataManagerImpl;
import org.apache.hadoop.hetu.om.OzoneManager;
import org.apache.hadoop.hetu.om.ratis.OzoneManagerDoubleBuffer;
import org.apache.hadoop.hetu.om.ratis.OzoneManagerRatisSnapshot;
import org.apache.hadoop.hetu.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.hetu.om.request.TestOMRequestUtils;
import org.apache.hadoop.hetu.om.request.database.OMDatabaseCreateRequest;
import org.apache.hadoop.hetu.om.request.table.OMTableCreateRequest;
import org.apache.hadoop.hetu.om.request.table.OMTableDeleteRequest;
import org.apache.hadoop.hetu.om.response.OMClientResponse;
import org.apache.hadoop.hetu.om.response.database.OMDatabaseCreateResponse;
import org.apache.hadoop.hetu.om.response.table.OMTableCreateResponse;
import org.apache.hadoop.hetu.om.response.table.OMTableDeleteResponse;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmTableInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.util.Daemon;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.hadoop.ozone.OzoneConsts.TRANSACTION_INFO_KEY;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * This class tests OzoneManagerDouble Buffer with actual OMResponse classes.
 */
public class TestHetuManagerDoubleBufferWithOMResponse {

  private static final int MAX_DATABASES = 1000;

  private OzoneManager ozoneManager;
  private OMMetrics omMetrics;
  private AuditLogger auditLogger;
  private OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper;
  private OMMetadataManager omMetadataManager;
  private OzoneManagerDoubleBuffer doubleBuffer;
  private final AtomicLong trxId = new AtomicLong(0);
  private OzoneManagerRatisSnapshot ozoneManagerRatisSnapshot;
  private volatile long lastAppliedIndex;
  private long term = 1L;

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Before
  public void setup() throws IOException {
    ozoneManager = Mockito.mock(OzoneManager.class,
        Mockito.withSettings().stubOnly());
    omMetrics = OMMetrics.create();
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        folder.newFolder().getAbsolutePath());
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration);
    when(ozoneManager.getMetrics()).thenReturn(omMetrics);
    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);
    when(ozoneManager.getMaxUserVolumeCount()).thenReturn(10L);
    auditLogger = Mockito.mock(AuditLogger.class);
    when(ozoneManager.getAuditLogger()).thenReturn(auditLogger);
    Mockito.doNothing().when(auditLogger).logWrite(any(AuditMessage.class));
    ozoneManagerRatisSnapshot = index -> {
      lastAppliedIndex = index.get(index.size() - 1);
    };
    doubleBuffer = new OzoneManagerDoubleBuffer.Builder().
        setOmMetadataManager(omMetadataManager).
        setOzoneManagerRatisSnapShot(ozoneManagerRatisSnapshot)
        .enableRatis(true)
        .setIndexToTerm((i) -> term)
        .build();
    ozoneManagerDoubleBufferHelper = doubleBuffer::add;
  }

  @After
  public void stop() {
    doubleBuffer.stop();
  }

  /**
   * This tests OzoneManagerDoubleBuffer implementation. It calls
   * testDoubleBuffer with number of iterations to do transactions and
   * number of buckets to be created in each iteration. It then
   * verifies OM DB entries count is matching with total number of
   * transactions or not.
   */
  @Test
  public void testDoubleBuffer1() throws Exception {
    testDoubleBuffer(1, 10);
  }

  @Test
  public void testDoubleBuffer10() throws Exception {
    testDoubleBuffer(10, 100);
  }

  @Test
  public void testDoubleBuffer100() throws Exception {
    testDoubleBuffer(100, 100);
  }

  @Test
  public void testDoubleBuffer1000() throws Exception {
    testDoubleBuffer(MAX_DATABASES, 500);
  }

  /**
   * This test first creates a volume, and then does a mix of transactions
   * like create/delete buckets and add them to double buffer. Then it
   * verifies OM DB entries are matching with actual responses added to
   * double buffer or not.
   */
  @Test
  public void testDoubleBufferWithMixOfTransactions() throws Exception {
    // This test checks count, data in table is correct or not.
    Queue<OMTableCreateResponse> tableQueue =
        new ConcurrentLinkedQueue<>();
    Queue<OMTableDeleteResponse> deleteTableQueue =
        new ConcurrentLinkedQueue<>();

    String databaseName = UUID.randomUUID().toString();
    OMDatabaseCreateResponse omDatabaseCreateResponse =
        (OMDatabaseCreateResponse) createDatabase(databaseName,
            trxId.incrementAndGet());

    int tableCount = 10;

    doMixTransactions(databaseName, tableCount, deleteTableQueue, tableQueue);

    // As for every 2 transactions of create table we add deleted table.
    final int deleteCount = 5;

    // We are doing +1 for database transaction.
    GenericTestUtils.waitFor(() ->
        doubleBuffer.getFlushedTransactionCount() ==
            (tableCount + deleteCount + 1), 100, 120000);

    Assert.assertEquals(1, omMetadataManager.countRowsInTable(
        omMetadataManager.getDatabaseTable()));

    Assert.assertEquals(5, omMetadataManager.countRowsInTable(
        omMetadataManager.getMetaTable()));

    // Now after this in our DB we should have 5 tables and one database

    checkDatabase(databaseName, omDatabaseCreateResponse);

    checkCreateTables(tableQueue);

    checkDeletedTables(deleteTableQueue);

    // Check lastAppliedIndex is updated correctly or not.
    Assert.assertEquals(tableCount + deleteCount + 1, lastAppliedIndex);


    TransactionInfo transactionInfo =
        omMetadataManager.getTransactionInfoTable().get(TRANSACTION_INFO_KEY);
    assertNotNull(transactionInfo);

    Assert.assertEquals(lastAppliedIndex,
        transactionInfo.getTransactionIndex());
    Assert.assertEquals(term, transactionInfo.getTerm());
  }

  /**
   * This test first creates a database, and then does a mix of transactions
   * like create/delete tables in parallel and add to double buffer. Then it
   * verifies OM DB entries are matching with actual responses added to
   * double buffer or not.
   */
  @Test
  public void testDoubleBufferWithMixOfTransactionsParallel() throws Exception {
    // This test checks count, data in table is correct or not.

    Queue<OMTableCreateResponse> tableQueue =
        new ConcurrentLinkedQueue<>();
    Queue<OMTableDeleteResponse> deleteTableQueue =
        new ConcurrentLinkedQueue<>();

    String databaseName1 = UUID.randomUUID().toString();
    OMDatabaseCreateResponse omDatabaseCreateResponse1 =
        (OMDatabaseCreateResponse) createDatabase(databaseName1,
            trxId.incrementAndGet());

    String databaseName2 = UUID.randomUUID().toString();
    OMDatabaseCreateResponse omDatabaseCreateResponse2 =
        (OMDatabaseCreateResponse) createDatabase(databaseName2,
            trxId.incrementAndGet());

    int tablesPerDatabase = 10;

    Daemon daemon1 = new Daemon(() -> doMixTransactions(databaseName1,
        tablesPerDatabase, deleteTableQueue, tableQueue));
    Daemon daemon2 = new Daemon(() -> doMixTransactions(databaseName2,
        tablesPerDatabase, deleteTableQueue, tableQueue));

    daemon1.start();
    daemon2.start();

    int tableCount = 2 * tablesPerDatabase;

      // As for every 2 transactions of create table we add deleted table.
    final int deleteCount = 10;

    // We are doing +1 for database transaction.
    GenericTestUtils.waitFor(() -> doubleBuffer.getFlushedTransactionCount()
            == (tableCount + deleteCount + 2), 100, 120000);

    Assert.assertEquals(2, omMetadataManager.countRowsInTable(
        omMetadataManager.getDatabaseTable()));

    Assert.assertEquals(10, omMetadataManager.countRowsInTable(
        omMetadataManager.getMetaTable()));

    // Now after this in our DB we should have 5 tables and one database


    checkDatabase(databaseName1, omDatabaseCreateResponse1);
    checkDatabase(databaseName2, omDatabaseCreateResponse2);

    checkCreateTables(tableQueue);

    checkDeletedTables(deleteTableQueue);

    // Not checking lastAppliedIndex here, because 2 daemon threads are
    // running in parallel, so lastAppliedIndex cannot be always
    // total transaction count. So, just checking here whether it is less
    // than total transaction count.
    Assert.assertTrue(lastAppliedIndex <= tableCount + deleteCount + 2);


  }

  /**
   * This method add's a mix of createTable/DeleteTable responses to double
   * buffer. Total number of responses added is specified by tableCount.
   */
  private void doMixTransactions(String database, int tableCount,
      Queue<OMTableDeleteResponse> deleteTableQueue,
      Queue<OMTableCreateResponse> tableQueue) {
    for (int i=0; i < tableCount; i++) {
      String tableName = UUID.randomUUID().toString();
      long transactionID = trxId.incrementAndGet();
      OMTableCreateResponse omTableCreateResponse = createTable(database,
          tableName, transactionID);
      // For every 2 transactions have a deleted table.
      if (i % 2 == 0) {
        OMTableDeleteResponse omTableDeleteResponse =
            (OMTableDeleteResponse) deleteTable(database, tableName,
                trxId.incrementAndGet());
        deleteTableQueue.add(omTableDeleteResponse);
      } else {
        tableQueue.add(omTableCreateResponse);
      }
    }
  }

  private OMClientResponse deleteTable(String databaseName, String tableName,
      long transactionID) {
    OzoneManagerProtocolProtos.OMRequest omRequest =
        TestOMRequestUtils.createDeleteTableRequest(databaseName, tableName);

    OMTableDeleteRequest omTableDeleteRequest =
        new OMTableDeleteRequest(omRequest);

    return omTableDeleteRequest.validateAndUpdateCache(ozoneManager,
        transactionID, ozoneManagerDoubleBufferHelper);
  }

  /**
   * Verifies database table data is matching with actual response added to
   * double buffer.
   */
  private void checkDatabase(String databaseName,
      OMDatabaseCreateResponse omDatabaseCreateResponse) throws Exception {
    OmDatabaseArgs tableDatabaseArgs = omMetadataManager.getDatabaseTable().get(
        omMetadataManager.getDatabaseKey(databaseName));
    Assert.assertNotNull(tableDatabaseArgs);

    OmDatabaseArgs omDatabaseArgs = omDatabaseCreateResponse.getOmDatabaseArgs();

    Assert.assertEquals(tableDatabaseArgs.getName(), omDatabaseArgs.getName());
    Assert.assertEquals(omDatabaseArgs.getAdminName(),
        tableDatabaseArgs.getAdminName());
    Assert.assertEquals(tableDatabaseArgs.getOwnerName(),
            omDatabaseArgs.getOwnerName());
    Assert.assertEquals(tableDatabaseArgs.getCreationTime(),
            omDatabaseArgs.getCreationTime());
  }

  /**
   * Verifies meta table data is matching with actual response added to
   * double buffer.
   */
  private void checkCreateTables(Queue<OMTableCreateResponse> tableQueue) {
    tableQueue.forEach((omTableCreateResponse) -> {
      OmTableInfo omTableInfo = omTableCreateResponse.getOmTableInfo();
      String tableName = omTableInfo.getTableName();
      OmTableInfo tableTableInfo = null;
      try {
        tableTableInfo =
            omMetadataManager.getMetaTable().get(
                omMetadataManager.getMetaTableKey(omTableInfo.getDatabaseName(),
                    tableName));
      } catch (IOException ex) {
        fail("testDoubleBufferWithMixOfTransactions failed");
      }
      Assert.assertNotNull(tableTableInfo);

      Assert.assertEquals(omTableInfo.getDatabaseName(),
          tableTableInfo.getDatabaseName());
      Assert.assertEquals(omTableInfo.getTableName(),
          tableTableInfo.getTableName());
      Assert.assertEquals(omTableInfo.getCreationTime(),
          tableTableInfo.getCreationTime());
    });
  }

  /**
   * Verifies deleted table responses added to double buffer are actually
   * removed from the OM DB or not.
   */
  private void checkDeletedTables(Queue<OMTableDeleteResponse>
      deleteTableQueue) {
    deleteTableQueue.forEach((omTableDeleteResponse -> {
      try {
        Assert.assertNull(omMetadataManager.getMetaTable().get(
            omMetadataManager.getMetaTableKey(
                omTableDeleteResponse.getDatabaseName(),
                    omTableDeleteResponse.getTableName())));
      } catch (IOException ex) {
        fail("testDoubleBufferWithMixOfTransactions failed");
      }
    }));
  }

  /**
   * Create tableCount number of createTable responses for each iteration.
   * All these iterations are run in parallel. Then verify OM DB has correct
   * number of entries or not.
   */
  private void testDoubleBuffer(int databaseCount, int tablesPerDatabase)
      throws Exception {
    // Reset transaction id.
    trxId.set(0);
    for (int i = 0; i < databaseCount; i++) {
      Daemon d1 = new Daemon(() -> doTransactions(tablesPerDatabase));
      d1.start();
    }

    // We are doing +1 for database transaction.
    // Here not checking lastAppliedIndex because transactionIndex is
    // shared across threads, and lastAppliedIndex cannot be always
    // expectedTransactions. So, skipping that check here.
    int expectedTables = tablesPerDatabase * databaseCount;
    long expectedTransactions = databaseCount + expectedTables;

    GenericTestUtils.waitFor(() ->
        expectedTransactions == doubleBuffer.getFlushedTransactionCount(),
        100, databaseCount * 500);

    GenericTestUtils.waitFor(() ->
        assertRowCount(databaseCount, omMetadataManager.getDatabaseTable()),
        300, databaseCount * 300);


    GenericTestUtils.waitFor(() ->
        assertRowCount(expectedTables, omMetadataManager.getMetaTable()),
        300, databaseCount * 300);

    Assert.assertTrue(doubleBuffer.getFlushIterations() > 0);
  }

  private boolean assertRowCount(int expected, Table<String, ?> table) {
    long count = 0L;
    try {
      count = omMetadataManager.countRowsInTable(table);
    } catch (IOException ex) {
      fail("testDoubleBuffer failed with: " + ex);
    }
    return count == expected;
  }

  /**
   * This method adds tableCount number of createTable responses to double
   * buffer.
   */
  private void doTransactions(int tableCount) {
    String databaseName = UUID.randomUUID().toString();
    createDatabase(databaseName, trxId.incrementAndGet());
    for (int i=0; i< tableCount; i++) {
      createTable(databaseName, UUID.randomUUID().toString(),
          trxId.incrementAndGet());
    }
  }

  /**
   * Create OMVolumeCreateResponse for specified database.
   * @return OMVolumeCreateResponse
   */
  private OMClientResponse createDatabase(String databaseName,
      long transactionId) {

    String admin = OzoneConsts.OZONE;
    String owner = UUID.randomUUID().toString();
    OzoneManagerProtocolProtos.OMRequest omRequest =
        TestOMRequestUtils.createDatabaseRequest(databaseName, admin, owner);

    OMDatabaseCreateRequest omDatabaseCreateRequest =
        new OMDatabaseCreateRequest(omRequest);

    return omDatabaseCreateRequest.validateAndUpdateCache(ozoneManager,
        transactionId, ozoneManagerDoubleBufferHelper);
  }

  /**
   * Create OMTableCreateResponse for specified database and table.
   * @return OMTableCreateResponse
   */
  private OMTableCreateResponse createTable(String databaseName,
      String tableName, long transactionID)  {

    OzoneManagerProtocolProtos.OMRequest omRequest =
        TestOMRequestUtils.createTableRequest(tableName, databaseName, false,
            OzoneManagerProtocolProtos.StorageTypeProto.DISK);

    OMTableCreateRequest omTableCreateRequest =
        new OMTableCreateRequest(omRequest);

    return (OMTableCreateResponse) omTableCreateRequest
        .validateAndUpdateCache(ozoneManager, transactionID,
            ozoneManagerDoubleBufferHelper);
  }

  /**
   * Create OMTableDeleteResponse for specified database and table.
   * @return OMTableDeleteResponse
   */
  private OMTableDeleteResponse deleteTable(String databaseName,
      String tableName) {
    return new OMTableDeleteResponse(OMResponse.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.DeleteTable)
        .setStatus(OzoneManagerProtocolProtos.Status.OK)
        .setDeleteTableResponse(OzoneManagerProtocolProtos.DeleteTableResponse.newBuilder().build())
        .build(), databaseName, tableName);
  }

}

