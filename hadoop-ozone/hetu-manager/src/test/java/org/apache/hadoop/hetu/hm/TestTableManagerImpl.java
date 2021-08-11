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
package org.apache.hadoop.hetu.hm;

import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.hetu.om.OmMetadataManagerImpl;
import org.apache.hadoop.hetu.om.TableManager;
import org.apache.hadoop.hetu.om.TableManagerImpl;
import org.apache.hadoop.hetu.om.request.TestOMRequestUtils;
import org.apache.hadoop.hetu.hm.helpers.OmDatabaseArgs;
import org.apache.hadoop.hetu.hm.meta.table.ColumnKey;
import org.apache.hadoop.hetu.hm.meta.table.ColumnSchema;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.helpers.OmPartitionInfo;
import org.apache.hadoop.ozone.om.helpers.OmTableArgs;
import org.apache.hadoop.ozone.om.helpers.OmTableInfo;
import org.apache.hadoop.util.Time;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import static org.apache.hadoop.hetu.om.request.TestOMRequestUtils.getColumnKeyProto;
import static org.apache.hadoop.hetu.om.request.TestOMRequestUtils.getPartitionsProto;

/**
 * Tests TableManagerImpl, mocks OMMetadataManager for testing.
 */
@RunWith(MockitoJUnitRunner.class)
@Ignore("Table Manager does not use cache, Disable it for now.")
public class TestTableManagerImpl {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private OzoneConfiguration createNewTestPath() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    File newFolder = folder.newFolder();
    if (!newFolder.exists()) {
      Assert.assertTrue(newFolder.mkdirs());
    }
    ServerUtils.setOzoneMetaDirPath(conf, newFolder.toString());
    return conf;
  }

  private OmMetadataManagerImpl createSampleDb() throws IOException {
    OzoneConfiguration conf = createNewTestPath();
    OmMetadataManagerImpl metaMgr = new OmMetadataManagerImpl(conf);
    String databaseKey = metaMgr.getDatabaseKey("sampleDb");
    // This is a simple hack for testing, we just test if the database via a
    // null check, do not parse the value part. So just write some dummy value.
    OmDatabaseArgs args =
        OmDatabaseArgs.newBuilder()
            .setName("sampleDb")
            .setAdminName("bilbo")
            .setOwnerName("bilbo")
            .build();
    metaMgr.getDatabaseTable().put(databaseKey, args);
    return metaMgr;
  }

  @Test
  public void testCreateTableWithoutDatabase() throws Exception {
    thrown.expectMessage("Database doesn't exist");
    OzoneConfiguration conf = createNewTestPath();
    OmMetadataManagerImpl metaMgr =
        new OmMetadataManagerImpl(conf);

    try {
      TableManager tableManager = new TableManagerImpl(metaMgr);
      OmTableInfo tableInfo = generateOmTableInfo();

      tableManager.createTable(tableInfo);
    } catch (OMException omEx) {
      Assert.assertEquals(ResultCodes.DATABASE_NOT_FOUND,
          omEx.getResult());
      throw omEx;
    } finally {
      metaMgr.getStore().close();
    }
  }

  private OmTableInfo generateOmTableInfo() {
    ColumnSchema col1 = new ColumnSchema(
            "city",
            "varchar(4)",
            "varcher",
            0,
            "",
            "",
            "城市",
            true);

    ColumnSchema col2 = new ColumnSchema(
            "id",
            "Long",
            "Long",
            1,
            "",
            "",
            "ID",
            true);

    return OmTableInfo.newBuilder()
            .setDatabaseName("sampleDb")
            .setTableName("tableOne")
            .setColumns(Arrays.asList(col1, col2))
            .setPartitions(getPartitionsProto())
            .setColumnKey(ColumnKey.fromProtobuf(getColumnKeyProto()))
            .setCreationTime(Time.now())
            .setUsedInBytes(0L).build();
  }

  @Test
  public void testCreateTable() throws Exception {
    OmMetadataManagerImpl metaMgr = createSampleDb();

    KeyProviderCryptoExtension kmsProvider = Mockito.mock(
        KeyProviderCryptoExtension.class);
    String testTableName = "table1";

    KeyProvider.Metadata mockMetadata = Mockito.mock(KeyProvider.Metadata
        .class);
    Mockito.when(kmsProvider.getMetadata(testTableName)).thenReturn(mockMetadata);

    TableManager tableManager = new TableManagerImpl(metaMgr,
        kmsProvider);
    OmTableInfo tableInfo = generateOmTableInfo();
    tableManager.createTable(tableInfo);
    Assert.assertNotNull(tableManager.getTableInfo("sampleDb", "tableOne"));

    metaMgr.getStore().close();
  }

  @Test
  public void testCreateAlreadyExistingTable() throws Exception {
    thrown.expectMessage("Table already exist");
    OmMetadataManagerImpl metaMgr = createSampleDb();

    try {
      TableManager tableManager = new TableManagerImpl(metaMgr);
      OmTableInfo tableInfo = OmTableInfo.newBuilder()
          .setDatabaseName("sampleDb")
          .setTableName("tableOne")
          .build();
      tableManager.createTable(tableInfo);
      tableManager.createTable(tableInfo);
    } catch (OMException omEx) {
      Assert.assertEquals(ResultCodes.TABLE_ALREADY_EXISTS,
          omEx.getResult());
      throw omEx;
    } finally {
      metaMgr.getStore().close();
    }
  }

  @Test
  public void testGetTableInfoForInvalidTable() throws Exception {
    thrown.expectMessage("Table not found");
    OmMetadataManagerImpl metaMgr = createSampleDb();
    try {


      TableManager tableManager = new TableManagerImpl(metaMgr);
      tableManager.getTableInfo("sampleDb", "tableOne");
    } catch (OMException omEx) {
      Assert.assertEquals(ResultCodes.TABLE_NOT_FOUND,
          omEx.getResult());
      throw omEx;
    } finally {
      metaMgr.getStore().close();
    }
  }

  @Test
  public void testGetTableInfo() throws Exception {
    OmMetadataManagerImpl metaMgr = createSampleDb();

    TableManager tableManager = new TableManagerImpl(metaMgr);
    OmTableInfo tableInfo = OmTableInfo.newBuilder()
        .setDatabaseName("sampleDb")
        .setTableName("tableOne")
        .setStorageType(StorageType.DISK)
        .setIsVersionEnabled(false)
        .build();
    createTable(metaMgr, tableInfo);
    OmTableInfo result = tableManager.getTableInfo(
        "sampleDb", "tableOne");
    Assert.assertEquals("sampleDb", result.getDatabaseName());
    Assert.assertEquals("tableOne", result.getTableName());
    Assert.assertEquals(StorageType.DISK,
        result.getStorageType());
    Assert.assertEquals(false, result.getIsVersionEnabled());
    metaMgr.getStore().close();
  }

  private void createTable(OMMetadataManager metadataManager,
      OmTableInfo tableInfo) throws IOException {
    TestOMRequestUtils.addMetaTableToOM(metadataManager, tableInfo);
  }

  @Test
  public void testSetTablePropertyChangeStorageType() throws Exception {
    OmMetadataManagerImpl metaMgr = createSampleDb();

    TableManager tableManager = new TableManagerImpl(metaMgr);
    OmTableInfo tableInfo = OmTableInfo.newBuilder()
        .setDatabaseName("sampleDb")
        .setTableName("tableOne")
        .setStorageType(StorageType.DISK)
        .build();
    createTable(metaMgr, tableInfo);
    OmTableInfo result = tableManager.getTableInfo(
        "sampleDb", "tableOne");
    Assert.assertEquals(StorageType.DISK,
        result.getStorageType());
    OmTableArgs tableArgs = OmTableArgs.newBuilder()
        .setDatabaseName("sampleDb")
        .setTableName("tableOne")
        .setStorageType(StorageType.SSD)
        .build();
    tableManager.setTableProperty(tableArgs);
    OmTableInfo updatedResult = tableManager.getTableInfo(
        "sampleDb", "tableOne");
    Assert.assertEquals(StorageType.SSD,
        updatedResult.getStorageType());
    metaMgr.getStore().close();
  }

  @Test
  public void testSetTablePropertyChangeVersioning() throws Exception {
    OmMetadataManagerImpl metaMgr = createSampleDb();

    TableManager tableManager = new TableManagerImpl(metaMgr);
    OmTableInfo tableInfo = OmTableInfo.newBuilder()
        .setDatabaseName("sampleDb")
        .setTableName("tableOne")
        .setIsVersionEnabled(false)
        .build();
    tableManager.createTable(tableInfo);
    OmTableInfo result = tableManager.getTableInfo(
        "sampleDb", "tableOne");
    Assert.assertFalse(result.getIsVersionEnabled());
    OmTableArgs tableArgs = OmTableArgs.newBuilder()
        .setDatabaseName("sampleDb")
        .setTableName("tableOne")
        .setIsVersionEnabled(true)
        .build();
    tableManager.setTableProperty(tableArgs);
    OmTableInfo updatedResult = tableManager.getTableInfo(
        "sampleDb", "tableOne");
    Assert.assertTrue(updatedResult.getIsVersionEnabled());
    metaMgr.getStore().close();
  }

  @Test
  public void testDeleteTable() throws Exception {
    thrown.expectMessage("Table not found");
    OmMetadataManagerImpl metaMgr = createSampleDb();
    TableManager tableManager = new TableManagerImpl(metaMgr);
    for (int i = 0; i < 5; i++) {
      OmTableInfo tableInfo = OmTableInfo.newBuilder()
          .setDatabaseName("sampleDb")
          .setTableName("table_" + i)
          .build();
      tableManager.createTable(tableInfo);
    }
    for (int i = 0; i < 5; i++) {
      Assert.assertEquals("table_" + i,
          tableManager.getTableInfo(
              "sampleDb", "table_" + i).getTableName());
    }
    try {
      tableManager.deleteTable("sampleDb", "table_1");
      Assert.assertNotNull(tableManager.getTableInfo(
          "sampleDb", "table_2"));
    } catch (IOException ex) {
      Assert.fail(ex.getMessage());
    }
    try {
      tableManager.getTableInfo("sampleDb", "table_1");
    } catch (OMException omEx) {
      Assert.assertEquals(ResultCodes.TABLE_NOT_FOUND,
          omEx.getResult());
      throw omEx;
    }
    metaMgr.getStore().close();
  }

  @Test
  public void testDeleteNonEmptyTable() throws Exception {
    thrown.expectMessage("Table is not empty");
    OmMetadataManagerImpl metaMgr = createSampleDb();
    TableManager tableManager = new TableManagerImpl(metaMgr);
    OmTableInfo tableInfo = OmTableInfo.newBuilder()
        .setDatabaseName("sampleDb")
        .setTableName("tableOne")
        .build();
    tableManager.createTable(tableInfo);
    //Create partitions in table
    metaMgr.getPartitionTable().put(metaMgr.getPartitionKey("sampleDb", "tableOne", "p1"),
        new OmPartitionInfo.Builder()
            .setDatabaseName("sampleDb")
            .setTableName("tableOne")
            .setPartitionName("p1")
            .setPartitionValue("20191011_18")
            .setBuckets(5)
            .setStorageType(StorageType.DEFAULT)
            .build());

    metaMgr.getPartitionTable().put(metaMgr.getPartitionKey("sampleDb", "tableOne", "p2"),
            new OmPartitionInfo.Builder()
                    .setDatabaseName("sampleDb")
                    .setTableName("tableOne")
                    .setPartitionName("p1")
                    .setPartitionValue("20191012_18")
                    .setBuckets(5)
                    .setStorageType(StorageType.DEFAULT)
                    .build());
    try {
      tableManager.deleteTable("sampleDb", "tableOne");
    } catch (OMException omEx) {
      Assert.assertEquals(ResultCodes.TABLE_NOT_EMPTY,
          omEx.getResult());
      throw omEx;
    }
    metaMgr.getStore().close();
  }
}
