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

import com.google.common.base.Optional;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.hetu.hm.helpers.OmDatabaseArgs;
import org.apache.hadoop.hetu.photon.meta.RuleType;
import org.apache.hadoop.hetu.photon.meta.common.ColumnKey;
import org.apache.hadoop.hetu.photon.meta.common.ColumnKeyType;
import org.apache.hadoop.hetu.photon.meta.common.ColumnType;
import org.apache.hadoop.hetu.photon.meta.common.ColumnTypeAttributes;
import org.apache.hadoop.hetu.photon.meta.common.DataType;
import org.apache.hadoop.hetu.photon.meta.table.ColumnSchema;
import org.apache.hadoop.hetu.photon.meta.table.DistributedKey;
import org.apache.hadoop.hetu.photon.meta.table.PartitionKey;
import org.apache.hadoop.hetu.photon.meta.table.Schema;
import org.apache.hadoop.hetu.photon.meta.common.StorageEngine;
import org.apache.hadoop.hetu.om.OmMetadataManagerImpl;
import org.apache.hadoop.hetu.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmTableInfo;
import org.apache.hadoop.ozone.om.helpers.OmTabletInfo;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OPEN_KEY_EXPIRE_THRESHOLD_SECONDS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OPEN_KEY_EXPIRE_THRESHOLD_SECONDS_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConsts.TRANSACTION_INFO_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_DB_DIRS;

/**
 * Tests HetuManager MetadataManager.
 */
public class TestHmMetadataManager extends TestBase {

  private OMMetadataManager omMetadataManager;
  private OzoneConfiguration ozoneConfiguration;

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();


  @Before
  public void setup() throws Exception {
    ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set(OZONE_OM_DB_DIRS,
        folder.getRoot().getAbsolutePath());
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration);
  }

  @Test
  public void testTransactionTable() throws Exception {
    omMetadataManager.getTransactionInfoTable().put(TRANSACTION_INFO_KEY,
        new TransactionInfo.Builder().setCurrentTerm(1)
            .setTransactionIndex(100).build());

    omMetadataManager.getTransactionInfoTable().put(TRANSACTION_INFO_KEY,
        new TransactionInfo.Builder().setCurrentTerm(2)
            .setTransactionIndex(200).build());

    omMetadataManager.getTransactionInfoTable().put(TRANSACTION_INFO_KEY,
        new TransactionInfo.Builder().setCurrentTerm(3)
            .setTransactionIndex(250).build());

    TransactionInfo transactionInfo =
        omMetadataManager.getTransactionInfoTable().get(TRANSACTION_INFO_KEY);

    Assert.assertEquals(3, transactionInfo.getTerm());
    Assert.assertEquals(250, transactionInfo.getTransactionIndex());
  }

  @Test
  public void testListDatabases() throws Exception {
    String ownerName = "owner";
    OmDatabaseArgs.Builder argsBuilder = OmDatabaseArgs.newBuilder()
        .setAdminName("admin")
        .setOwnerName(ownerName);

    String dbName;
    OmDatabaseArgs omDatabaseArgs;
    for (int i = 0; i < 50; i++) {
      dbName = "db" + i;
      omDatabaseArgs = argsBuilder
          .setName(dbName)
          .build();

      TestOMRequestUtils.addDatabaseToOM(omMetadataManager, omDatabaseArgs);
      TestOMRequestUtils.addDbUserToDB(dbName, ownerName, omMetadataManager);
    }

    // Test list databases with setting startDatabase that
    // was not part of the result.
    String prefix = "";
    int totalDb = omMetadataManager
        .listDatabase(ownerName, prefix, null, 100)
        .size();
    int startOrder = 10;
    String startDatabase = "db" + startOrder;
    List<OmDatabaseArgs> databaseList = omMetadataManager.listDatabase(ownerName,
        prefix, startDatabase, 100);
    Assert.assertEquals(databaseList.size(), totalDb - startOrder - 1);
  }

  @Test
  public void testListAllDatabases() throws Exception {
    OmDatabaseArgs.Builder argsBuilder =
        OmDatabaseArgs.newBuilder().setAdminName("admin");
    String dbName;
    String ownerName;
    for (int i = 0; i < 50; i++) {
      ownerName = "owner" + i;
      dbName = "dba" + i;
      OmDatabaseArgs omDatabaseArgs = argsBuilder.
          setOwnerName(ownerName).setName(dbName).build();
      TestOMRequestUtils.addDatabaseToOM(omMetadataManager, omDatabaseArgs);
      TestOMRequestUtils.addDbUserToDB(dbName, ownerName, omMetadataManager);
    }
    for (int i = 0; i < 50; i++) {
      ownerName = "owner" + i;
      dbName = "dbb" + i;
      OmDatabaseArgs omDatabaseArgs = argsBuilder.
          setOwnerName(ownerName).setName(dbName).build();
      TestOMRequestUtils.addDatabaseToOM(omMetadataManager, omDatabaseArgs);
      TestOMRequestUtils.addDbUserToDB(dbName, ownerName, omMetadataManager);
    }

    String prefix = "";
    String startKey = "";

    // Test list all databases
    List<OmDatabaseArgs> volListA = omMetadataManager.listDatabase(null,
        prefix, startKey, 1000);
    Assert.assertEquals(volListA.size(), 100);

    // Test list all database with prefix
    prefix = "dbb";
    List<OmDatabaseArgs> dbListB = omMetadataManager.listDatabase(null,
        prefix, startKey, 1000);
    Assert.assertEquals(dbListB.size(), 50);

    // Test list all databases with setting startDatabase
    // that was not part of result.
    prefix = "";
    int totalDb = dbListB.size();
    int startOrder = 0;
    startKey = "dbb" + startOrder;
    List<OmDatabaseArgs> volListC = omMetadataManager.listDatabase(null,
        prefix, startKey, 1000);
    Assert.assertEquals(volListC.size(), totalDb - startOrder - 1);
  }

  @Test
  public void testListTables() throws Exception {
    String databaseName1 = "databaseA";
    String prefixTableNameWithOzoneOwner = "ozoneTable";
    String prefixTableNameWithHadoopOwner = "hadoopTable";

    TestOMRequestUtils.addDatabaseToDB(databaseName1, omMetadataManager);


    TreeSet<String> databaseATablesPrefixWithOzoneOwner = new TreeSet<>();
    TreeSet<String> databaseATablesPrefixWithHadoopOwner = new TreeSet<>();

    // Add exact name in prefixTableNameWithOzoneOwner without postfix.
    databaseATablesPrefixWithOzoneOwner.add(prefixTableNameWithOzoneOwner);
    addTablesToCache(databaseName1, prefixTableNameWithOzoneOwner);
    for (int i = 1; i < 100; i++) {
      if (i % 2 == 0) { // This part adds 49 buckets.
        databaseATablesPrefixWithOzoneOwner.add(
            prefixTableNameWithOzoneOwner + i);
        addTablesToCache(databaseName1, prefixTableNameWithOzoneOwner + i);
      } else {
        databaseATablesPrefixWithHadoopOwner.add(
            prefixTableNameWithHadoopOwner + i);
        addTablesToCache(databaseName1, prefixTableNameWithHadoopOwner + i);
      }
    }

    String databaseName2 = "databaseB";
    TreeSet<String> databaseBTablesPrefixWithOzoneOwner = new TreeSet<>();
    TreeSet<String> databaseBTablesPrefixWithHadoopOwner = new TreeSet<>();
    TestOMRequestUtils.addDatabaseToDB(databaseName2, omMetadataManager);

    // Add exact name in prefixTableNameWithOzoneOwner without postfix.
    databaseBTablesPrefixWithOzoneOwner.add(prefixTableNameWithOzoneOwner);
    addTablesToCache(databaseName2, prefixTableNameWithOzoneOwner);
    for (int i = 1; i < 100; i++) {
      if (i % 2 == 0) { // This part adds 49 tables.
        databaseBTablesPrefixWithOzoneOwner.add(
            prefixTableNameWithOzoneOwner + i);
        addTablesToCache(databaseName2, prefixTableNameWithOzoneOwner + i);
      } else {
        databaseBTablesPrefixWithHadoopOwner.add(
            prefixTableNameWithHadoopOwner + i);
        addTablesToCache(databaseName2, prefixTableNameWithHadoopOwner + i);
      }
    }

    // List all tables which have prefix ozoneTable
    List<OmTableInfo> omTableInfoList =
        omMetadataManager.listMetaTables(databaseName1,
            null, prefixTableNameWithOzoneOwner, 100);

    // Cause adding a exact name in prefixTableNameWithOzoneOwner
    // and another 49 tables, so if we list tables with --prefix
    // prefixTableNameWithOzoneOwner, we should get 50 tables.
    Assert.assertEquals(omTableInfoList.size(), 50);

    for (OmTableInfo omTableInfo : omTableInfoList) {
      Assert.assertTrue(omTableInfo.getTableName().startsWith(
          prefixTableNameWithOzoneOwner));
    }


    String startTable = prefixTableNameWithOzoneOwner + 10;
    omTableInfoList =
        omMetadataManager.listMetaTables(databaseName1,
            startTable, prefixTableNameWithOzoneOwner,
            100);

    Assert.assertEquals(databaseATablesPrefixWithOzoneOwner.tailSet(
        startTable).size() - 1, omTableInfoList.size());

    startTable = prefixTableNameWithOzoneOwner + 38;
    omTableInfoList =
        omMetadataManager.listMetaTables(databaseName1,
            startTable, prefixTableNameWithOzoneOwner,
            100);

    Assert.assertEquals(databaseATablesPrefixWithOzoneOwner.tailSet(
        startTable).size() - 1, omTableInfoList.size());

    for (OmTableInfo omTableInfo : omTableInfoList) {
      Assert.assertTrue(omTableInfo.getTableName().startsWith(
          prefixTableNameWithOzoneOwner));
      Assert.assertFalse(omTableInfo.getTableName().equals(
          prefixTableNameWithOzoneOwner + 10));
    }



    omTableInfoList = omMetadataManager.listMetaTables(databaseName2,
        null, prefixTableNameWithHadoopOwner, 100);

    // Cause adding a exact name in prefixTableNameWithOzoneOwner
    // and another 49 tables, so if we list tables with --prefix
    // prefixTableNameWithOzoneOwner, we should get 50 tables.
    Assert.assertEquals(omTableInfoList.size(), 50);

    for (OmTableInfo omTableInfo : omTableInfoList) {
      Assert.assertTrue(omTableInfo.getTableName().startsWith(
          prefixTableNameWithHadoopOwner));
    }

    // Try to get tables by count 10, like that get all tables in the
    // databaseB with prefixTableNameWithHadoopOwner.
    startTable = null;
    TreeSet<String> expectedTables = new TreeSet<>();
    for (int i=0; i<5; i++) {
      omTableInfoList = omMetadataManager.listMetaTables(databaseName2,
          startTable, prefixTableNameWithHadoopOwner, 10);

      Assert.assertEquals(omTableInfoList.size(), 10);

      for (OmTableInfo omTableInfo : omTableInfoList) {
        expectedTables.add(omTableInfo.getTableName());
        Assert.assertTrue(omTableInfo.getTableName().startsWith(
            prefixTableNameWithHadoopOwner));
        startTable =  omTableInfo.getTableName();
      }
    }


    Assert.assertEquals(databaseBTablesPrefixWithHadoopOwner, expectedTables);
    // As now we have iterated all 50 tables, calling next time should
    // return empty list.
    omTableInfoList = omMetadataManager.listMetaTables(databaseName2,
        startTable, prefixTableNameWithHadoopOwner, 10);

    Assert.assertEquals(omTableInfoList.size(), 0);
  }


  private void addTablesToCache(String databaseName, String tableName) {

    Schema schema = new Schema(getColumnSchemas(), getColumnKey(), getDistributedKey(), getPartitionKey());

    OmTableInfo omTableInfo = OmTableInfo.newBuilder()
        .setDatabaseName(databaseName)
        .setTableName(tableName)
        .setSchema(schema)
        .setStorageEngine(StorageEngine.LSTORE)
        .setIsVersionEnabled(false)
        .setBuckets(8)
        .build();

    omMetadataManager.getMetaTable().addCacheEntry(
        new CacheKey<>(omMetadataManager.getMetaTableKey(databaseName, tableName)),
        new CacheValue<>(Optional.of(omTableInfo), 1));
  }

  @Test
  public void testListTablets() throws Exception {

    String databaseNameA = "databaseA";
    String databaseNameB = "databaseB";
    String ozoneTable = "ozoneTable";
    String hadoopTable = "hadoopTable";
    String ozoneTestTable = "ozoneTable-Test";
    String ozonePartition = "op-20100120";
    String hadoopPartition = "hp-20100120";
    String ozoneTestPartition = "otp-20100120";

    // Create databases, partition and tables.
    TestOMRequestUtils.addDatabaseToDB(databaseNameA, omMetadataManager);
    TestOMRequestUtils.addDatabaseToDB(databaseNameB, omMetadataManager);
    addTablesToCache(databaseNameA, ozoneTable);
    addTablesToCache(databaseNameB, hadoopTable);
    addTablesToCache(databaseNameA, ozoneTestTable);
    TestOMRequestUtils.addPartitionToDB(databaseNameA, ozoneTable, ozonePartition, omMetadataManager);
    TestOMRequestUtils.addPartitionToDB(databaseNameB, hadoopTable, hadoopPartition, omMetadataManager);
    TestOMRequestUtils.addPartitionToDB(databaseNameA, ozoneTestTable, ozoneTestPartition, omMetadataManager);

    String prefixTabletA = "tablet-a";
    String prefixTabletB = "tablet-b";
    String prefixTabletC = "tablet-c";
    TreeSet<String> tabletsASet = new TreeSet<>();
    TreeSet<String> tabletsBSet = new TreeSet<>();
    TreeSet<String> tabletsCSet = new TreeSet<>();
    for (int i=1; i<= 100; i++) {
      if (i % 2 == 0) {
        tabletsASet.add(
            prefixTabletA + i);
        addTabletsToOM(databaseNameA, ozoneTable, ozonePartition, prefixTabletA + i, i);
      } else {
        tabletsBSet.add(
            prefixTabletB + i);
        addTabletsToOM(databaseNameA, hadoopTable, hadoopPartition, prefixTabletB + i, i);
      }
    }
    tabletsCSet.add(prefixTabletC + 1);
    addTabletsToOM(databaseNameA, ozoneTestTable, ozoneTestPartition, prefixTabletC + 0, 0);

    TreeSet<String> tabletsADatabaseBSet = new TreeSet<>();
    TreeSet<String> tabletsBDatabaseBSet = new TreeSet<>();
    for (int i=1; i<= 100; i++) {
      if (i % 2 == 0) {
        tabletsADatabaseBSet.add(
            prefixTabletA + i);
        addTabletsToOM(databaseNameB, ozoneTable, ozonePartition, prefixTabletA + i, i);
      } else {
        tabletsBDatabaseBSet.add(
            prefixTabletB + i);
        addTabletsToOM(databaseNameB, hadoopTable, hadoopPartition, prefixTabletB + i, i);
      }
    }


    // List all tablet which have prefix "tablet-a"
    List<OmTabletInfo> omTabletInfoList =
        omMetadataManager.listTablets(databaseNameA, ozoneTable,
            ozonePartition, null, prefixTabletA, 100);

    Assert.assertEquals(omTabletInfoList.size(),  50);

    for (OmTabletInfo omTabletInfo : omTabletInfoList) {
      Assert.assertTrue(omTabletInfo.getTabletName().startsWith(
          prefixTabletA));
    }


    String startKey = prefixTabletA + 10;
    omTabletInfoList =
        omMetadataManager.listTablets(databaseNameA, ozoneTable, ozonePartition,
            startKey, prefixTabletA, 100);

    Assert.assertEquals(tabletsASet.tailSet(
        startKey).size() - 1, omTabletInfoList.size());

    startKey = prefixTabletA + 38;
    omTabletInfoList =
        omMetadataManager.listTablets(databaseNameA, ozoneTable, ozonePartition,
            startKey, prefixTabletA, 100);

    Assert.assertEquals(tabletsASet.tailSet(
        startKey).size() - 1, omTabletInfoList.size());

    for (OmTabletInfo omTabletInfo : omTabletInfoList) {
      Assert.assertTrue(omTabletInfo.getTabletName().startsWith(
          prefixTabletA));
      Assert.assertFalse(omTabletInfo.getPartitionName().equals(
          prefixTabletA + 38));
    }


    omTabletInfoList = omMetadataManager.listTablets(databaseNameB, hadoopTable, hadoopPartition,
        null, prefixTabletB, 100);

    Assert.assertEquals(omTabletInfoList.size(),  50);

    for (OmTabletInfo omTabletInfo : omTabletInfoList) {
      Assert.assertTrue(omTabletInfo.getTabletName().startsWith(
          prefixTabletB));
    }

    // Try to get tablets by count 10, like that get all tablets in the
    // databaseB/ozoneTable/ozonePartition with "tablet-a".
    startKey = null;
    TreeSet<String> expectedTablets = new TreeSet<>();
    for (int i=0; i<5; i++) {

      omTabletInfoList = omMetadataManager.listTablets(databaseNameB, hadoopTable, hadoopPartition,
          startKey, prefixTabletB, 10);

      Assert.assertEquals(10, omTabletInfoList.size());

      for (OmTabletInfo omTabletInfo : omTabletInfoList) {
        expectedTablets.add(omTabletInfo.getTabletName());
        Assert.assertTrue(omTabletInfo.getTabletName().startsWith(
            prefixTabletB));
        startKey =  omTabletInfo.getTabletName();
      }
    }

    Assert.assertEquals(expectedTablets, tabletsBDatabaseBSet);


    // As now we have iterated all 50 tablets, calling next time should
    // return empty list.
    omTabletInfoList = omMetadataManager.listTablets(databaseNameB, hadoopTable, hadoopPartition,
        startKey, prefixTabletB, 10);

    Assert.assertEquals(omTabletInfoList.size(), 0);

    // List all tablets with empty prefix
    omTabletInfoList = omMetadataManager.listTablets(databaseNameA, ozoneTable, ozonePartition,
        null, null, 100);
    Assert.assertEquals(50, omTabletInfoList.size());
    for (OmTabletInfo omTabletInfo : omTabletInfoList) {
      Assert.assertTrue(omTabletInfo.getTabletName().startsWith(
          prefixTabletA));
    }
  }

  @Test
  public void testListTabletsWithFewDeleteEntriesInCache() throws Exception {
    String databaseNameA = "databaseA";
    String ozoneTable = "ozoneTable";
    String ozonePartition = "op-20100120";

    // Create databases, table and partition.
    TestOMRequestUtils.addDatabaseToDB(databaseNameA, omMetadataManager);
    addTablesToCache(databaseNameA, ozoneTable);
    TestOMRequestUtils.addPartitionToDB(databaseNameA, ozoneTable, ozonePartition, omMetadataManager);

    String prefixTabletA = "tablet-a";
    TreeSet<String> tabletsASet = new TreeSet<>();
    TreeSet<String> deleteTabletSet = new TreeSet<>();


    for (int i=1; i<= 100; i++) {
      if (i % 2 == 0) {
        tabletsASet.add(
            prefixTabletA + i);
        addTabletsToOM(databaseNameA, ozoneTable, ozonePartition, prefixTabletA + i, i);
      } else {
        addTabletsToOM(databaseNameA, ozoneTable, ozonePartition,prefixTabletA + i, i);
        String tablet = omMetadataManager.getOzoneTablet(databaseNameA,
            ozoneTable, ozonePartition, prefixTabletA + i);
        // Mark as deleted in cache.
        omMetadataManager.getTabletTable().addCacheEntry(
            new CacheKey<>(tablet),
            new CacheValue<>(Optional.absent(), 100L));
        deleteTabletSet.add(tablet);
      }
    }

    // Now list tablets which match with prefixTabletA.
    List<OmTabletInfo> omTabletInfoList =
        omMetadataManager.listTablets(databaseNameA, ozoneTable, ozonePartition,
            null, prefixTabletA, 100);

    // As in total 100, 50 are marked for delete. It should list only 50 tablets.
    Assert.assertEquals(50, omTabletInfoList.size());

    TreeSet<String> expectedTablets = new TreeSet<>();

    for (OmTabletInfo omTabletInfo : omTabletInfoList) {
      expectedTablets.add(omTabletInfo.getTabletName());
      Assert.assertTrue(omTabletInfo.getTabletName().startsWith(prefixTabletA));
    }

    Assert.assertEquals(expectedTablets, tabletsASet);


    // Now get tablet count by 10.
    String startKey = null;
    expectedTablets = new TreeSet<>();
    for (int i=0; i<5; i++) {

      omTabletInfoList = omMetadataManager.listTablets(databaseNameA, ozoneTable, ozonePartition,
          startKey, prefixTabletA, 10);

      System.out.println(i);
      Assert.assertEquals(10, omTabletInfoList.size());

      for (OmTabletInfo omTabletInfo : omTabletInfoList) {
        expectedTablets.add(omTabletInfo.getTabletName());
        Assert.assertTrue(omTabletInfo.getTabletName().startsWith(
            prefixTabletA));
        startKey =  omTabletInfo.getTabletName();
      }
    }

    Assert.assertEquals(tabletsASet, expectedTablets);


    // As now we have iterated all 50 tables, calling next time should
    // return empty list.
    omTabletInfoList = omMetadataManager.listTablets(databaseNameA, ozoneTable, ozonePartition,
        startKey, prefixTabletA, 10);

    Assert.assertEquals(omTabletInfoList.size(), 0);
  }

  @Test
  public void testGetExpiredOpenTablets() throws Exception {
    final String databaseName = "database";
    final String tableName = "table";
    final String partitionName = "op-20101021";
    final int numExpiredOpenTablets = 4;
    final int numUnexpiredOpenTablets = 1;
    final long clientID = 1000L;
    // To create expired tablets, they will be assigned a creation time twice as
    // old as the minimum expiration time.
    final long minExpiredTimeSeconds = ozoneConfiguration.getInt(
            OZONE_OPEN_KEY_EXPIRE_THRESHOLD_SECONDS,
            OZONE_OPEN_KEY_EXPIRE_THRESHOLD_SECONDS_DEFAULT);
    final long expiredAgeMillis =
            Instant.now().minus(minExpiredTimeSeconds * 2,
                    ChronoUnit.SECONDS).toEpochMilli();

    // Add expired tablets to open tablet table.
    // The method under test does not check for expired open tablets in the
    // cache, since they will be picked up once the cache is flushed.
    Set<String> expiredTablets = new HashSet<>();
    for (int i = 0; i < numExpiredOpenTablets; i++) {
      OmTabletInfo tabletInfo = TestOMRequestUtils.createOmTabletInfo(databaseName,
              tableName, partitionName, "expired" + i, HddsProtos.ReplicationType.RATIS,
              HddsProtos.ReplicationFactor.ONE, 0L, expiredAgeMillis);

      TestOMRequestUtils.addTabletToTable(true, false,
              tabletInfo, clientID, 0L, omMetadataManager);

      String groupID = omMetadataManager.getOpenTablet(databaseName, tableName, partitionName,
              tabletInfo.getTabletName(), clientID);
      expiredTablets.add(groupID);
    }

    // Add unexpired tablets to open tablet table.
    for (int i = 0; i < numUnexpiredOpenTablets; i++) {
      OmTabletInfo tabletInfo = TestOMRequestUtils.createOmTabletInfo(databaseName, tableName,
              partitionName, "unexpired" + i, HddsProtos.ReplicationType.RATIS,
              HddsProtos.ReplicationFactor.ONE);

      TestOMRequestUtils.addTabletToTable(true, false,
              tabletInfo, clientID, 0L, omMetadataManager);
    }

    // Test retrieving fewer expired tablets than actually exist.
    List<String> someExpiredTablets =
            omMetadataManager.getExpiredOpenTablets(numExpiredOpenTablets - 1);

    Assert.assertEquals(numExpiredOpenTablets - 1, someExpiredTablets.size());
    for (String tablet: someExpiredTablets) {
      Assert.assertTrue(expiredTablets.contains(tablet));
    }

    // Test attempting to retrieving more expired tablets than actually exist.
    List<String> allExpiredTablets =
            omMetadataManager.getExpiredOpenTablets(numExpiredOpenTablets + 1);

    Assert.assertEquals(numExpiredOpenTablets, allExpiredTablets.size());
    for (String tablet: allExpiredTablets) {
      Assert.assertTrue(expiredTablets.contains(tablet));
    }

    // Test retrieving exact amount of expired tablets that exist.
    allExpiredTablets =
            omMetadataManager.getExpiredOpenTablets(numExpiredOpenTablets);

    Assert.assertEquals(numExpiredOpenTablets, allExpiredTablets.size());
    for (String tablet: allExpiredTablets) {
      Assert.assertTrue(expiredTablets.contains(tablet));
    }
  }

  private void addTabletsToOM(String databaseName, String tableName, String partitionName,
      String tabletName, int i) throws Exception {

    if (i%2== 0) {
      TestOMRequestUtils.addTabletToTable(false, databaseName, tableName, partitionName, tabletName,
          1000L, HddsProtos.ReplicationType.RATIS,
          HddsProtos.ReplicationFactor.ONE, omMetadataManager);
    } else {
      TestOMRequestUtils.addTabletToTableCache(databaseName, tableName, partitionName, tabletName,
          HddsProtos.ReplicationType.RATIS, HddsProtos.ReplicationFactor.ONE,
          omMetadataManager);
    }
  }

}