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

/**
 * One tablet can be stored in one or more containers as one or more blocks.
 * This class represents one such block instance.
 */
public class OzoneTabletLocation {
  /**
   * Which container this tablet stored.
   */
  private final long containerID;
  /**
   * Which block this tablet stored inside a container.
   */
  private final long localID;
  /**
   * Data length of this tablet replica.
   */
  private final long length;
  /**
   * Offset of this tablet.
   */
  private final long offset;
  /**
   * TabletOffset of this tablet.
   */
  private final long tabletOffset;
  /**
   * Constructs OzoneTabletLocation.
   */
  public OzoneTabletLocation(long containerID, long localID,
                             long length, long offset, long tabletOffset) {
    this.containerID = containerID;
    this.localID = localID;
    this.length = length;
    this.offset = offset;
    this.tabletOffset = tabletOffset;
  }

  /**
   * Returns the containerID of this Tablet.
   */
  public long getContainerID() {
    return containerID;
  }

  /**
   * Returns the localID of this Tablet.
   */
  public long getLocalID() {
    return localID;
  }

  /**
   * Returns the length of this Tablet.
   */
  public long getLength() {
    return length;
  }

  /**
   * Returns the offset of this Tablet.
   */
  public long getOffset() {
    return offset;
  }

  /**
   * Returns the tabletOffset of this Tablet.
   */
  public long getTabletOffset() {
    return tabletOffset;
  }

}
