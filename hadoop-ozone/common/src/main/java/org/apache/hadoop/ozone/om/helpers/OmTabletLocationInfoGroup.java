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
package org.apache.hadoop.ozone.om.helpers;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TabletLocationList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A list of tablet locations. This class represents one single version of the
 * blocks of a tablet.
 */
public class OmTabletLocationInfoGroup {
  private final long version;
  private final Map<Long, List<OmTabletLocationInfo>> locationVersionMap;

  public OmTabletLocationInfoGroup(long version,
                                   List<OmTabletLocationInfo> locations) {
    this.version = version;
    locationVersionMap = new HashMap<>();
    for (OmTabletLocationInfo info : locations) {
      locationVersionMap
          .computeIfAbsent(info.getCreateVersion(), v -> new ArrayList<>())
          .add(info);
    }
    //prevent NPE
    this.locationVersionMap.putIfAbsent(version, new ArrayList<>());

  }

  public OmTabletLocationInfoGroup(long version,
                                   Map<Long, List<OmTabletLocationInfo>> locations) {
    this.version = version;
    this.locationVersionMap = locations;
    //prevent NPE
    this.locationVersionMap.putIfAbsent(version, new ArrayList<>());
  }

  /**
   * Return only the blocks that are created in the most recent version.
   *
   * @return the list of blocks that are created in the latest version.
   */
  public List<OmTabletLocationInfo> getBlocksLatestVersionOnly() {
    return new ArrayList<>(locationVersionMap.get(version));
  }

  public long getVersion() {
    return version;
  }

  public List<OmTabletLocationInfo> getLocationList() {
    return locationVersionMap.values().stream().flatMap(List::stream)
        .collect(Collectors.toList());
  }

  public long getLocationListCount() {
    return locationVersionMap.values().stream().mapToLong(List::size).sum();
  }

  public List<OmTabletLocationInfo> getLocationList(Long versionToFetch) {
    return new ArrayList<>(locationVersionMap.get(versionToFetch));
  }

  public TabletLocationList getProtobuf(boolean ignorePipeline,
      int clientVersion) {
    TabletLocationList.Builder builder = TabletLocationList.newBuilder()
        .setVersion(version);
    List<OzoneManagerProtocolProtos.TabletLocation> tabletLocationList =
        new ArrayList<>();
    for (List<OmTabletLocationInfo> locationList : locationVersionMap.values()) {
      for (OmTabletLocationInfo tabletInfo : locationList) {
        tabletLocationList.add(tabletInfo.getProtobuf(ignorePipeline, clientVersion));
      }
    }
    return  builder.addAllTabletLocation(tabletLocationList).build();
  }

  public static OmTabletLocationInfoGroup getFromProtobuf(
      TabletLocationList tabletLocationList) {
    return new OmTabletLocationInfoGroup(
        tabletLocationList.getVersion(),
        tabletLocationList.getTabletLocationList().stream()
            .map(OmTabletLocationInfo::getFromProtobuf)
            .collect(Collectors.groupingBy(
                OmTabletLocationInfo::getCreateVersion))
    );
  }

  /**
   * Given a new block location, generate a new version list based upon this
   * one.
   *
   * @param newLocationList a list of new location to be added.
   * @return newly generated OmTabletLocationInfoGroup
   */
  OmTabletLocationInfoGroup generateNextVersion(
      List<OmTabletLocationInfo> newLocationList) {
    Map<Long, List<OmTabletLocationInfo>> newMap =
        new HashMap<>(locationVersionMap);
    newMap.put(version + 1, new ArrayList<>(newLocationList));
    return new OmTabletLocationInfoGroup(version + 1, newMap);
  }

  void appendNewBlocks(List<OmTabletLocationInfo> newLocationList) {
    List<OmTabletLocationInfo> locationList = locationVersionMap.get(version);
    for (OmTabletLocationInfo info : newLocationList) {
      info.setCreateVersion(version);
      locationList.add(info);
    }
  }

  void removeBlocks(long versionToRemove){
    locationVersionMap.remove(versionToRemove);
  }

  void addAll(long versionToAdd, List<OmTabletLocationInfo> locationInfoList) {
    locationVersionMap.putIfAbsent(versionToAdd, new ArrayList<>());
    List<OmTabletLocationInfo> list = locationVersionMap.get(versionToAdd);
    list.addAll(locationInfoList);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("version:").append(version).append(" ");
    for (List<OmTabletLocationInfo> kliList : locationVersionMap.values()) {
      for(OmTabletLocationInfo kli: kliList) {
        sb.append(kli.getLocalID()).append(" || ");
      }
    }
    return sb.toString();
  }
}
