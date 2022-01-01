/**
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

package org.apache.hadoop.ozone.om.helpers;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneTabletStatusProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneTabletStatusProto.Builder;

import java.util.Objects;

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;

/**
 * Tablet Status of the Ozone.
 */
public class OzoneTabletStatus {

  private static final long serialVersionUID = 1L;

  /**
   * The tablet info object for blocks. Leave null for the root directory.
   */
  private OmTabletInfo tabletInfo;

  private long blockSize;

  public OzoneTabletStatus(OmTabletInfo tabletInfo,
                           long blockSize) {
    this.tabletInfo = tabletInfo;
    this.blockSize = blockSize;
  }

  public OmTabletInfo getTabletInfo() {
    return tabletInfo;
  }

  public long getBlockSize() {
    return blockSize;
  }

  public String getTrimmedName() {
    String tabletName = tabletInfo.getTabletName();
    if (tabletName.endsWith(OZONE_URI_DELIMITER)) {
      return tabletName.substring(0, tabletName.length() - 1);
    } else {
      return tabletName;
    }
  }

  public String getPath() {
    if (tabletInfo == null) {
      return OZONE_URI_DELIMITER;
    } else {
      String path = OZONE_URI_DELIMITER + tabletInfo.getTabletName();
      if (path.endsWith(OZONE_URI_DELIMITER)) {
        return path.substring(0, path.length() - 1);
      } else {
        return path;
      }
    }
  }

  public OzoneTabletStatusProto getProtobuf(int clientVersion) {
    Builder builder = OzoneTabletStatusProto.newBuilder()
        .setBlockSize(blockSize);
    //tablet info can be null for the fake root entry.
    if (tabletInfo != null) {
      builder.setTabletInfo(tabletInfo.getProtobuf(clientVersion));
    }
    return builder.build();
  }

  public static OzoneTabletStatus getFromProtobuf(OzoneTabletStatusProto status) {
    return new OzoneTabletStatus(
        OmTabletInfo.getFromProtobuf(status.getTabletInfo()),
        status.getBlockSize());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof OzoneTabletStatus)) {
      return false;
    }
    OzoneTabletStatus that = (OzoneTabletStatus) o;
    return blockSize == that.blockSize &&
        getTrimmedName().equals(that.getTrimmedName());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getTrimmedName());
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getClass().getSimpleName());
    sb.append("{");
    if (tabletInfo == null) {
      sb.append("<root>");
    } else {
      sb.append(getTrimmedName());
    }
    sb.append("}");
    return sb.toString();
  }

}
