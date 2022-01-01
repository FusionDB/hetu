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

package org.apache.hadoop.hetu.client;

import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.InMemoryConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hetu.client.io.HetuInputStream;
import org.apache.hadoop.hetu.client.io.HetuOutputStream;
import org.apache.hadoop.hetu.photon.helpers.PartialRow;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.hetu.client.rpc.RpcClient;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.protocolPB.OmTransport;
import org.apache.ozone.test.LambdaTestUtils.VoidCallable;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

/**
 * Real unit test for HetuClient.
 * <p>
 * Used for testing Hetu client without external network calls.
 */
public class TestHetuClient extends TestHetuUtil {
  static final Logger LOG = LoggerFactory.getLogger(TestHetuUtil.class);

  private HetuClient client;
  private HetuStore store;

  public static <E extends Throwable> void expectOmException(
      ResultCodes code,
      VoidCallable eval)
      throws Exception {
    try {
      eval.call();
      Assert.fail("OMException is expected");
    } catch (OMException ex) {
      Assert.assertEquals(code, ex.getResult());
    }
  }

  @Before
  public void init() throws IOException {
    ConfigurationSource config = new InMemoryConfiguration();
    client = new HetuClient(config, new RpcClient(config, null) {

      @Override
      protected OmTransport createOmTransport(
          String omServiceId)
          throws IOException {
        return new MockOmTransport();
      }

      @NotNull
      @Override
      protected XceiverClientFactory createXceiverClientFactory(
          List<X509Certificate> x509Certificates)
          throws IOException {
        return new MockXceiverClientFactory();
      }
    });

    store = client.getObjectStore();
  }

  @After
  public void close() throws IOException {
    client.close();
  }

  @Test
  public void testDeleteDatabase()
      throws Exception {
    String databaseName = UUID.randomUUID().toString();
    store.createDatabase(databaseName);
    OzoneDatabase database = store.getDatabase(databaseName);
    Assert.assertNotNull(database);
    store.deleteDatabase(databaseName);
    expectOmException(ResultCodes.DATABASE_NOT_FOUND,
        () -> store.getDatabase(databaseName));
  }

  @Test
  public void testCreateDatabaseWithMetadata()
      throws IOException, HetuClientException {
    String databaseName = UUID.randomUUID().toString();
    DatabaseArgs databaseArgs = DatabaseArgs.newBuilder()
        .addMetadata("key1", "val1")
        .build();
    store.createDatabase(databaseName, databaseArgs);
    OzoneDatabase database = store.getDatabase(databaseName);
    Assert.assertEquals(OzoneConsts.QUOTA_RESET, database.getQuotaInTable());
    Assert.assertEquals(OzoneConsts.QUOTA_RESET, database.getQuotaInBytes());
    Assert.assertEquals("val1", database.getMetadata().get("key1"));
    Assert.assertEquals(databaseName, database.getDatabaseName());
  }

  @Test
  public void testCreateTable()
      throws IOException {
    Instant testStartTime = Instant.now();
    String databaseName = UUID.randomUUID().toString();
    String tableName = UUID.randomUUID().toString();
    store.createDatabase(databaseName);
    OzoneDatabase database = store.getDatabase(databaseName);
    TableArgs tableArgs = builderTableArgs(databaseName, tableName);
    database.createTable(tableName, tableArgs);
    OzoneTable table = database.getTable(tableName);
    Assert.assertEquals(tableName, table.getTableName());
    Assert.assertFalse(table.getCreationTime().isBefore(testStartTime));
    Assert.assertFalse(database.getCreationTime().isBefore(testStartTime));
  }

  @Test
  public void testPutTabletRatisOneNode() throws IOException {
    String databaseName = UUID.randomUUID().toString();
    String tableName = UUID.randomUUID().toString();
    Instant testStartTime = Instant.now();

//    String value = "sample value";
    PartialRow row = getPartialRowWithAllTypes();
    byte[] data = row.toProtobuf().toByteArray();

    store.createDatabase(databaseName);
    OzoneDatabase database = store.getDatabase(databaseName);
    TableArgs tableArgs = builderTableArgs(databaseName, tableName);
    database.createTable(tableName, tableArgs);
    OzoneTable table = database.getTable(tableName);

    PartitionArgs partitionArgs = PartitionArgs.newBuilder()
            .setPartitionName("ds20201112")
            .setTableName(tableName)
            .setDatabaseName(databaseName)
            .setPartitionValue("20201112")
            .setStorageType(table.getStorageType())
            .build();
    table.createPartition("ds20201112", partitionArgs);
    OzonePartition partition = table.getPartition("ds20201112");

    for (int i = 0; i < 10; i++) {
      String tabletName = UUID.randomUUID().toString();
      HetuOutputStream out = partition.createTablet(tabletName,
          data.length,
          new RatisReplicationConfig(HddsProtos.ReplicationFactor.ONE),
          new HashMap<>());
//      out.write(value.getBytes(UTF_8));
      out.write(data);
      out.close();
      OzoneTablet tablet = partition.getTablet(tabletName);
      Assert.assertEquals(tabletName, tablet.getTabletName());
      HetuInputStream is = partition.readTablet(tabletName);
      byte[] fileContent = new byte[data.length];
      Assert.assertEquals(data.length, is.read(fileContent));
      is.close();

      // TODO: data by mock data node storage
      LOG.info("Read length {} from tablet name {}", fileContent.length, tableName);
      PartialRow partialRow = PartialRow.fromPersistedFormat(fileContent);
      Assert.assertEquals(partialRow.getVarLengthData().size(), row.getVarLengthData().size());
      Assert.assertFalse(tablet.getCreationTime().isBefore(testStartTime));
      Assert.assertFalse(tablet.getModificationTime().isBefore(testStartTime));
    }
  }

  @Test
  public void testInsertAndScanOneNode() throws IOException, HetuClientException {
    String databaseName = UUID.randomUUID().toString();
    String tableName = UUID.randomUUID().toString();

    store.createDatabase(databaseName);
    OzoneDatabase database = store.getDatabase(databaseName);
    TableArgs tableArgs = builderTableArgs(databaseName, tableName);
    database.createTable(tableName, tableArgs);

    for (int i = 0; i < 10; i++) {
      PartialRow row = generateRowData();
      database.insertTable(tableName, row);
    }

    List<PartialRow> partialRows = database.scanQueryTable(tableName, null);
    Assert.assertTrue(partialRows.size() == 10);
  }
}