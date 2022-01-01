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

package org.apache.hadoop.hetu.client;

import org.apache.hadoop.hdds.client.ReplicationType;

import java.time.Instant;

/**
 * A class that encapsulates OzoneTablet.
 */
public class OzoneTablet {

  /**
   * Name of the Database the Tablet belongs to.
   */
  private final String databaseName;
  /**
   * Name of the Table the Tablet belongs to.
   */
  private final String tableName;
  /**
   * Name of the Partition the Tablet belongs to.
   */
  private final String partitionName;
  /**
   * Name of the Tablet.
   */
  private final String tabletName;
  /**
   * Size of the data.
   */
  private final long dataSize;
  /**
   * Creation time of the key.
   */
  private Instant creationTime;
  /**
   * Modification time of the key.
   */
  private Instant modificationTime;

  private ReplicationType replicationType;

  private int replicationFactor;

  /**
   * Constructs OzoneKey from OmKeyInfo.
   *
   */
  @SuppressWarnings("parameternumber")
  public OzoneTablet(String databaseName, String tableName, String partitionName,
                     String tabletName, long size, long creationTime,
                     long modificationTime, ReplicationType type,
                     int replicationFactor) {
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.partitionName = partitionName;
    this.tabletName = tabletName;
    this.dataSize = size;
    this.creationTime = Instant.ofEpochMilli(creationTime);
    this.modificationTime = Instant.ofEpochMilli(modificationTime);
    this.replicationType = type;
    this.replicationFactor = replicationFactor;
  }

  /**
   * Returns Database Name associated with the Tablet.
   *
   * @return databaseName
   */
  public String getDatabaseName() {
    return databaseName;
  }

  /**
   * Returns Table Name associated with the Tablet.
   *
   * @return tableName
   */
  public String getTableName(){
    return tableName;
  }

  /**
   * Returns Partition Name associated with the Tablet.
   *
   * @return partitionName
   */
  public String getPartitionName(){
    return partitionName;
  }

  /**
   * Returns the Tablet Name.
   *
   * @return tabletName
   */
  public String getTabletName() {
    return tabletName;
  }

  /**
   * Returns the size of the data.
   *
   * @return dataSize
   */
  public long getDataSize() {
    return dataSize;
  }

  /**
   * Returns the creation time of the key.
   *
   * @return creation time
   */
  public Instant getCreationTime() {
    return creationTime;
  }

  /**
   * Returns the modification time of the key.
   *
   * @return modification time
   */
  public Instant getModificationTime() {
    return modificationTime;
  }

  /**
   * Returns the replication type of the key.
   *
   * @return replicationType
   */
  public ReplicationType getReplicationType() {
    return replicationType;
  }

  /**
   * Returns the replication factor of the key.
   *
   * @return replicationFactor
   */
  public int getReplicationFactor() {
    return replicationFactor;
  }

}