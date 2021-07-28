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

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TabletInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RepeatedTabletInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * Args for deleted tablets. This is written to om metadata deletedTablet.
 * Once a tablet is deleted, it is moved to om metadata deletedTablet. Having a
 * {label: List<OMTabletInfo>} ensures that if users create & delete tablets with
 * exact same uri multiple times, all the delete instances are bundled under
 * the same tablet name. This is useful as part of GDPR compliance where an
 * admin wants to confirm if a given tablet is deleted from deletedTablet metadata.
 */
public class RepeatedOmTabletInfo {
  private List<OmTabletInfo> omTabletInfoList;

  public RepeatedOmTabletInfo(List<OmTabletInfo> omTabletInfos) {
    this.omTabletInfoList = omTabletInfos;
  }

  public RepeatedOmTabletInfo(OmTabletInfo omTabletInfos) {
    this.omTabletInfoList = new ArrayList<>();
    this.omTabletInfoList.add(omTabletInfos);
  }

  public void addOmTabletInfo(OmTabletInfo info) {
    this.omTabletInfoList.add(info);
  }

  public List<OmTabletInfo> getOmTabletInfoList() {
    return omTabletInfoList;
  }

  public static RepeatedOmTabletInfo getFromProto(RepeatedTabletInfo
      repeatedTabletInfo) {
    List<OmTabletInfo> list = new ArrayList<>();
    for(TabletInfo k : repeatedTabletInfo.getTabletInfoList()) {
      list.add(OmTabletInfo.getFromProtobuf(k));
    }
    return new RepeatedOmTabletInfo.Builder().setOmTabletInfos(list).build();
  }

  /**
   *
   * @param compact, true for persistence, false for network transmit
   * @return
   */
  public RepeatedTabletInfo getProto(boolean compact, int clientVersion) {
    List<TabletInfo> list = new ArrayList<>();
    for(OmTabletInfo k : omTabletInfoList) {
      list.add(k.getProtobuf(compact, clientVersion));
    }

    RepeatedTabletInfo.Builder builder = RepeatedTabletInfo.newBuilder()
        .addAllTabletInfo(list);
    return builder.build();
  }

  /**
   * Builder of RepeatedOmTabletInfo.
   */
  public static class Builder {
    private List<OmTabletInfo> omTabletInfos;

    public Builder(){}

    public Builder setOmTabletInfos(List<OmTabletInfo> infoList) {
      this.omTabletInfos = infoList;
      return this;
    }

    public RepeatedOmTabletInfo build() {
      return new RepeatedOmTabletInfo(omTabletInfos);
    }
  }
}
