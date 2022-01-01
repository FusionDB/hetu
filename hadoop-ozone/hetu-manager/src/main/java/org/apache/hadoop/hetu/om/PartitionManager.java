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
package org.apache.hadoop.hetu.om;

import org.apache.hadoop.ozone.om.helpers.OmPartitionArgs;
import org.apache.hadoop.ozone.om.helpers.OmPartitionInfo;

import java.io.IOException;
import java.util.List;

/**
 * PartitionManager handles all the partition level operations.
 */
public interface PartitionManager extends IOzoneAcl {
  /**
   * Creates a partition.
   * @param partitionInfo - OmPartitionInfo for creating partition.
   */
  void createPartition(OmPartitionInfo partitionInfo) throws IOException;

  /**
   * Returns Partition Information.
   * @param databaseName - Name of the Database.
   * @param tableName - Name of the Table.
   * @param partitionName - Name of the Partition.
   */
  OmPartitionInfo getPartitionInfo(String databaseName, String tableName, String partitionName)
      throws IOException;

  /**
   * Sets partition property from args.
   * @param args - OmPartitionArgs.
   * @throws IOException
   */
  void setPartitionProperty(OmPartitionArgs args) throws IOException;

  /**
   * Deletes an existing empty partition from database.
   * @param databaseName - Name of the Database.
   * @param tableName - Name of the Table.
   * @param partitionName - Name of the Partition.
   * @throws IOException
   */
  void deletePartition(String databaseName, String tableName, String partitionName) throws IOException;

  /**
   * Returns a list of partitions represented by {@link OmPartitionInfo}
   * in the given table.
   *
   * @param databaseName
   *   Required parameter database name determines tables in which database
   *   to return.
   * @param tableName
   *   Required parameter table name determines partitions in which table
   *   to return.
   * @param startPartition
   *   Optional start partition name parameter indicating where to start
   *   the partition listing from, this key is excluded from the result.
   * @param partitionPrefix
   *   Optional start key parameter, restricting the response to partitions
   *   that begin with the specified name.
   * @param maxNumOfPartitions
   *   The maximum number of partitions to return. It ensures
   *   the size of the result will not exceed this limit.
   * @return a list of partitions.
   * @throws IOException
   */
  List<OmPartitionInfo> listPartitions(String databaseName, String tableName,
      String startPartition, String partitionPrefix, int maxNumOfPartitions)
      throws IOException;

}
