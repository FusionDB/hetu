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

package org.apache.hadoop.ozone.om.helpers;

import java.util.List;

/**
 * Represent class which has info of Tablets to be deleted from Client.
 */
public class OmDeleteTablets {

  private String databaseName;
  private String tableName;
  private String partitionName;

  private List<String> tabletNames;


  public OmDeleteTablets(String databaseName, String tableName,
    String partitionName, List<String> tabletNames) {
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.partitionName = partitionName;
    this.tabletNames = tabletNames;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public String getTableName() {
    return tableName;
  }

  public String getPartitionName() {
    return partitionName;
  }

  public List<String> getTabletNames() {
    return tabletNames;
  }
}
