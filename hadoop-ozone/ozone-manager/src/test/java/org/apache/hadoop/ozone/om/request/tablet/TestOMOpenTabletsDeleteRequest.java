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

package org.apache.hadoop.ozone.om.request.tablet;

import com.google.common.base.Optional;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.hetu.hm.helpers.OmDatabaseArgs;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.helpers.OmTabletInfo;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteOpenTabletsRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OpenTablet;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OpenTabletPartition;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * Tests OMOpenTabletsDeleteRequest.
 */
public class TestOMOpenTabletsDeleteRequest extends TestOMTabletRequest {
  /**
   * Tests removing tablets from the open tablet table cache that never existed there.
   * The operation should complete without errors.
   * <p>
   * This simulates a run of the open tablet cleanup service where a set of
   * expired open tablets are identified and passed to the request, but before
   * the request can process them, those tablets are committed and removed from
   * the open tablet table.
   * @throws Exception
   */
  @Test
  public void testDeleteOpenTabletsNotInTable() throws Exception {
    OpenTabletPartition openTablets = makeOpenTablets(databaseName, tableName, partitionName, 5);
    deleteOpenTabletsFromCache(openTablets);
    assertNotInOpenTabletTable(openTablets);
  }

  /**
   * Tests adding multiple tablets to the open tablet table, and updating the table
   * cache to only remove some of them.
   * Tablets not removed should still be present in the open tablet table.
   * Mixes which tablets will be kept and deleted among different databases and
   * tables.
   * @throws Exception
   */
  @Test
  public void testDeleteSubsetOfOpenTablets() throws Exception {
    final String db1 = "db1";
    final String tab1 = "tab1";
    final String p1 = "p1";
    final String db2 = "db2";
    final String tab2 = "tab2";
    final String p2 = "p2";

    OpenTabletPartition v1b1TabletsToDelete = makeOpenTablets(db1, tab1, p1, 3);
    OpenTabletPartition v1b1TabletsToKeep = makeOpenTablets(db1, tab1, p1, 3);

    OpenTabletPartition v1b2TabletsToDelete = makeOpenTablets(db1, tab2, p2, 3);
    OpenTabletPartition v1b2TabletsToKeep = makeOpenTablets(db1, tab2, p2, 3);

    OpenTabletPartition v2b2TabletsToDelete = makeOpenTablets(db2, tab2, p2, 3);
    OpenTabletPartition v2b2TabletsToKeep = makeOpenTablets(db2, tab2, p2, 3);

    addToOpenTabletTableDB(
        v1b1TabletsToKeep,
        v1b2TabletsToKeep,
        v2b2TabletsToKeep,
        v1b1TabletsToDelete,
        v1b2TabletsToDelete,
        v2b2TabletsToDelete
    );

    deleteOpenTabletsFromCache(
        v1b1TabletsToDelete,
        v1b2TabletsToDelete,
        v2b2TabletsToDelete
    );

    assertNotInOpenTabletTable(
        v1b1TabletsToDelete,
        v1b2TabletsToDelete,
        v2b2TabletsToDelete
    );

    assertInOpenTabletTable(
        v1b1TabletsToKeep,
        v1b2TabletsToKeep,
        v2b2TabletsToKeep
    );
  }

  /**
   * Tests removing tablets from the open tablet table cache that have the same
   * name, but different client IDs.
   * @throws Exception
   */
  @Test
  public void testDeleteSameTabletNameDifferentClient() throws Exception {
    OpenTabletPartition tabletsToKeep =
        makeOpenTablets(databaseName, tableName, partitionName, tabletName, 3);
    OpenTabletPartition tabletsToDelete =
        makeOpenTablets(databaseName, tableName, partitionName, tabletName, 1);

    addToOpenTabletTableDB(tabletsToKeep, tabletsToDelete);
    deleteOpenTabletsFromCache(tabletsToDelete);

    assertNotInOpenTabletTable(tabletsToDelete);
    assertInOpenTabletTable(tabletsToKeep);
  }

  /**
   * Tests metrics set by {@link OMOpenTabletsDeleteRequest}.
   * Submits a set of tablets for deletion where only some of the tablets actually
   * exist in the open tablet table, and asserts that the metrics count tablets
   * that were submitted for deletion versus those that were actually deleted.
   * @throws Exception
   */
  @Test
  public void testMetrics() throws Exception {
    final int numExistentTablets = 3;
    final int numNonExistentTablets = 5;

    OMMetrics metrics = ozoneManager.getMetrics();
    Assert.assertEquals(metrics.getNumOpenTabletDeleteRequests(), 0);
    Assert.assertEquals(metrics.getNumOpenTabletDeleteRequestFails(), 0);
    Assert.assertEquals(metrics.getNumOpenTabletsSubmittedForDeletion(), 0);
    Assert.assertEquals(metrics.getNumOpenTabletsDeleted(), 0);

    OpenTabletPartition existentTablets =
        makeOpenTablets(databaseName, tabletName, partitionName, tabletName, numExistentTablets);
    OpenTabletPartition nonExistentTablets =
        makeOpenTablets(databaseName, tabletName, partitionName, tabletName, numNonExistentTablets);

    addToOpenTabletTableDB(existentTablets);
    deleteOpenTabletsFromCache(existentTablets, nonExistentTablets);

    assertNotInOpenTabletTable(existentTablets);
    assertNotInOpenTabletTable(nonExistentTablets);

    Assert.assertEquals(1, metrics.getNumOpenTabletDeleteRequests());
    Assert.assertEquals(0, metrics.getNumOpenTabletDeleteRequestFails());
    Assert.assertEquals(numExistentTablets + numNonExistentTablets,
        metrics.getNumOpenTabletsSubmittedForDeletion());
    Assert.assertEquals(numExistentTablets, metrics.getNumOpenTabletsDeleted());
  }

  /**
   * Runs the validate and update cache step of
   * {@link OMOpenTabletsDeleteRequest} to mark the tabelts in {@code openTablets}
   * as deleted in the open tablet table cache.
   * Asserts that the call's response status is {@link Status#OK}.
   * @throws Exception
   */
  private void deleteOpenTabletsFromCache(OpenTabletPartition... openTablets)
      throws Exception {

    OMRequest omRequest =
        doPreExecute(createDeleteOpenTabletRequest(openTablets));

    OMOpenTabletsDeleteRequest openTabletDeleteRequest =
        new OMOpenTabletsDeleteRequest(omRequest);

    OMClientResponse omClientResponse =
        openTabletDeleteRequest.validateAndUpdateCache(ozoneManager,
            100L, ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(Status.OK,
        omClientResponse.getOMResponse().getStatus());
  }

  /**
   * Adds {@code openTablets} to the open tablet table DB only, and asserts that they
   * are present after the addition.
   * @throws Exception
   */
  private void addToOpenTabletTableDB(OpenTabletPartition... openTablets)
      throws Exception {

    addToOpenTabletTableDB(0, openTablets);
  }

  /**
   * Adds {@code openTablets to the open tablet table DB only, and asserts that they
   * are present after the addition. Adds each tablet to the table with a single
   * block of size {@code tabletSize}.
   * @throws Exception
   */
  private void addToOpenTabletTableDB(long tabletSize, OpenTabletPartition... openTablets)
      throws Exception {

    for (OpenTabletPartition openTabletPartition: openTablets) {
      String database = openTabletPartition.getDatabaseName();
      String table = openTabletPartition.getTableName();
      String partition = openTabletPartition.getPartitionName();

      for (OpenTablet openTablet: openTabletPartition.getTabletsList()) {
        if (tabletSize > 0) {
          OmTabletInfo tabletInfo = TestOMRequestUtils.createOmTabletInfo(database, table, partition,
              openTablet.getName(), replicationType, replicationFactor);
          TestOMRequestUtils.addTabletLocationInfo(tabletInfo,  0, tabletSize);

          TestOMRequestUtils.addTabletToTable(true, false,
              tabletInfo, openTablet.getClientID(), 0L, omMetadataManager);
        } else {
          TestOMRequestUtils.addTabletToTable(true,
                  database, table, partition, openTablet.getName(), openTablet.getClientID(),
              replicationType, replicationFactor, omMetadataManager);
        }
      }
    }

    assertInOpenTabletTable(openTablets);
  }

  /**
   * Constructs a list of {@link OpenTabletPartition} objects of size {@code numTablets}.
   * The tablets created will all have the same database and table, but
   * randomized tablet names and client IDs. These tablets are not added to the
   * open tablet table.
   *
   * @param database The database all open tablets created will have.
   * @param table The table all open tables created will have.
   * @param partition The partition all open tables created will have.
   * @param numTablets The number of tablets with randomized tablet names and client
   * IDs to create.
   * @return A list of new open tablets with size {@code numTablets}.
   */
  private OpenTabletPartition makeOpenTablets(String database, String table, String partition,
      int numTablets) {

    OpenTabletPartition.Builder tabletsPerPartitionBuilder =
        OpenTabletPartition.newBuilder()
        .setDatabaseName(database)
        .setTableName(table)
        .setPartitionName(partition);

    for (int i = 0; i < numTablets; i++) {
      String tabletName = UUID.randomUUID().toString();
      long clientID = random.nextLong();

      OpenTablet openTablet = OpenTablet.newBuilder()
          .setName(tabletName)
          .setClientID(clientID)
          .build();
      tabletsPerPartitionBuilder.addTablets(openTablet);
    }

    return tabletsPerPartitionBuilder.build();
  }

  /**
   * Constructs a list of {@link OpenTablet} objects of size {@code numTablets}.
   * The tablets created will all have the same database, table, partition, and
   * tablet names, but randomized client IDs. These tablets are not added to the
   * open tablet table.
   *
   * @param database The database all open tablets created will have.
   * @param table The table all open tablets created will have.
   * @param partition The partition all open tablets created will have.
   * @param tablet The tablet name all open tablets created will have.
   * @param numTablets The number of tablets with randomized tablet names and client
   * IDs to create.
   * @return A list of new open tablets with size {@code numTablets}.
   */
  private OpenTabletPartition makeOpenTablets(String database, String table, String partition,
      String tablet, int numTablets) {

    OpenTabletPartition.Builder tabletsPerPartitionBuilder =
            OpenTabletPartition.newBuilder()
            .setDatabaseName(database)
            .setTableName(table)
            .setPartitionName(partition);

    for (int i = 0; i < numTablets; i++) {
      long clientID = random.nextLong();

      OpenTablet openTablet = OpenTablet.newBuilder()
          .setName(tablet)
          .setClientID(clientID)
          .build();
      tabletsPerPartitionBuilder.addTablets(openTablet);
    }

    return tabletsPerPartitionBuilder.build();
  }

  private void assertInOpenTabletTable(OpenTabletPartition... openTablets)
      throws Exception {

    for (String tabletName: getFullOpenTabletNames(openTablets)) {
      Assert.assertTrue(omMetadataManager.getOpenTabletTable().isExist(tabletName));
    }
  }

  private void assertNotInOpenTabletTable(OpenTabletPartition... openTablets)
      throws Exception {

    for (String tabletName: getFullOpenTabletNames(openTablets)) {
      Assert.assertFalse(omMetadataManager.getOpenTabletTable().isExist(tabletName));
    }
  }

  /**
   * Expands all the open tablets represented by {@code openTabletPartitions} to their
   * full
   * tablet names as strings.
   * @param openTabletPartitions
   * @return
   */
  private List<String> getFullOpenTabletNames(OpenTabletPartition... openTabletPartitions) {
    List<String> fullTabletNames = new ArrayList<>();

    for(OpenTabletPartition tabletsPerPartition: openTabletPartitions) {
      String database = tabletsPerPartition.getDatabaseName();
      String table = tabletsPerPartition.getTableName();
      String partition = tabletsPerPartition.getPartitionName();

      for (OpenTablet openTablet: tabletsPerPartition.getTabletsList()) {
        String fullName = omMetadataManager.getOpenTablet(database, table, partition,
            openTablet.getName(), openTablet.getClientID());
        fullTabletNames.add(fullName);
      }
    }

    return fullTabletNames;
  }

  /**
   * Constructs a new {@link OMOpenTabletsDeleteRequest} objects, and calls its
   * {@link OMOpenTabletsDeleteRequest#preExecute} method with {@code
   * originalOMRequest}. It verifies that {@code originalOMRequest} is modified
   * after the call, and returns it.
   * @throws Exception
   */
  private OMRequest doPreExecute(OMRequest originalOmRequest) throws Exception {
    OMOpenTabletsDeleteRequest omOpenTabletsDeleteRequest =
        new OMOpenTabletsDeleteRequest(originalOmRequest);

    OMRequest modifiedOmRequest =
        omOpenTabletsDeleteRequest.preExecute(ozoneManager);

    // Will not be equal, as UserInfo will be set.
    Assert.assertNotEquals(originalOmRequest, modifiedOmRequest);

    return modifiedOmRequest;
  }

  /**
   * Creates an {@code OpenTabletDeleteRequest} to delete the tablets represented by
   * {@code tabletsToDelete}. Returns an {@code OMRequest} which encapsulates this
   * {@code OpenTabletDeleteRequest}.
   */
  private OMRequest createDeleteOpenTabletRequest(OpenTabletPartition... tabletsToDelete) {
    DeleteOpenTabletsRequest deleteOpenTabletsRequest =
        DeleteOpenTabletsRequest.newBuilder()
            .addAllOpenTabletsPerPartition(Arrays.asList(tabletsToDelete))
            .build();

    return OMRequest.newBuilder()
        .setDeleteOpenTabletsRequest(deleteOpenTabletsRequest)
        .setCmdType(OzoneManagerProtocolProtos.Type.DeleteOpenKeys)
        .setClientId(UUID.randomUUID().toString()).build();
  }

  private void addDatabaseToCacheAndDB(OmDatabaseArgs omDatabaseArgs) throws Exception {
    String databaseKey = omMetadataManager.getDatabaseKey(omDatabaseArgs.getName());

    omMetadataManager.getDatabaseTable().addCacheEntry(
        new CacheKey<>(databaseKey),
        new CacheValue<>(Optional.of(omDatabaseArgs), omDatabaseArgs.getUpdateID())
    );

    omMetadataManager.getDatabaseTable().put(databaseKey, omDatabaseArgs);
  }

  private OmDatabaseArgs getDatabaseFromDB(String database) throws Exception {
    String databaseKey = omMetadataManager.getDatabaseKey(database);
    return omMetadataManager.getDatabaseTable().getSkipCache(databaseKey);
  }

  private OmDatabaseArgs getDatabaseFromCache(String database) {
    String databaseKey = omMetadataManager.getDatabaseKey(database);
    CacheValue<OmDatabaseArgs> value = omMetadataManager.getDatabaseTable()
        .getCacheValue(new CacheKey<>(databaseKey));

    OmDatabaseArgs result = null;
    if (value != null) {
      result = value.getCacheValue();
    }

    return result;
  }
}
