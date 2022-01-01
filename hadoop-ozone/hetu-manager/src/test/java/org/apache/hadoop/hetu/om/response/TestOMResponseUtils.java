/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.hetu.om.response;

import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmPartitionInfo;
import org.apache.hadoop.ozone.om.helpers.OmTableInfo;
import org.apache.hadoop.util.Time;

import static org.apache.hadoop.hetu.om.request.TestOMRequestUtils.getSchema;

/**
 * Helper class to test OMClientResponse classes.
 */
public final class TestOMResponseUtils {

  // No one can instantiate, this is just utility class with all static methods.
  private TestOMResponseUtils() {
  }

  public static  OmBucketInfo createBucket(String volume, String bucket) {
    return OmBucketInfo.newBuilder().setVolumeName(volume).setBucketName(bucket)
        .setCreationTime(Time.now()).setIsVersionEnabled(true).addMetadata(
            "key1", "value1").build();

  }

  public static OmTableInfo createTable(String database, String table) {
    return OmTableInfo.newBuilder().setDatabaseName(database).setTableName(table)
            .setSchema(getSchema())
            .setStorageEngine(StorageEngine.LSTORE)
            .setNumReplicas(3)
            .setCreationTime(Time.now()).setIsVersionEnabled(true).addMetadata(
                    "key1", "value1").build();

  }

  public static OmPartitionInfo createPartition(String databaseName, String tableName,
                                                String partitionName, String partitionValue) {
      return OmPartitionInfo.newBuilder()
              .setDatabaseName(databaseName)
              .setTableName(tableName)
              .setPartitionName(partitionName)
              .setPartitionValue(partitionValue)
              .setSizeInBytes(-1)
              .setRows(0L)
              .setBuckets(8)
              .setCreationTime(Time.now())
              .setModificationTime(Time.now())
              .setStorageType(StorageType.DISK)
              .setIsVersionEnabled(true)
              .addMetadata("key1", "value1")
              .build();
  }
}
