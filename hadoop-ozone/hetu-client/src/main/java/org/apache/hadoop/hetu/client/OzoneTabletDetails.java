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

import java.util.List;
import java.util.Map;

/**
 * A class that encapsulates OzoneTabletLocation.
 */
public class OzoneTabletDetails extends OzoneTablet {

  /**
   * A list of block location information to specify replica locations.
   */
  private List<OzoneTabletLocation> ozoneTabletLocations;

  private Map<String, String> metadata;

  /**
   * Constructs OzoneTabletDetails from OmTabletInfo.
   */
  @SuppressWarnings("parameternumber")
  public OzoneTabletDetails(String databaseName, String tableName, String partitionName, String tabletName,
                            long size, long creationTime, long modificationTime,
                            List<OzoneTabletLocation> ozoneTabletLocations,
                            ReplicationType type, Map<String, String> metadata,
                            int replicationFactor) {
    super(databaseName, tableName, partitionName, tabletName, size, creationTime,
        modificationTime, type, replicationFactor);
    this.ozoneTabletLocations = ozoneTabletLocations;
    this.metadata = metadata;
  }

  /**
   * Returns the location detail information of the specific Tablet.
   */
  public List<OzoneTabletLocation> getOzoneTabletLocations() {
    return ozoneTabletLocations;
  }

  public Map<String, String> getMetadata() {
    return metadata;
  }

  /**
   * Set details of tablet location.
   * @param ozoneTabletLocations - details of tablet location
   */
  public void setOzoneTabletLocations(List<OzoneTabletLocation> ozoneTabletLocations) {
    this.ozoneTabletLocations = ozoneTabletLocations;
  }
}
