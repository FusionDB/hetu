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
package org.apache.hadoop.ozone.security.auth;

import com.google.common.base.Preconditions;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneObj.ObjectType;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneObj.StoreType.valueOf;

/**
 * Class representing an unique ozone object.
 * */
public abstract class HetuObj implements IHetuObj {

  private final ResourceType resType;

  private final StoreType storeType;

  HetuObj(ResourceType resType, StoreType storeType) {

    Preconditions.checkNotNull(resType);
    Preconditions.checkNotNull(storeType);
    this.resType = resType;
    this.storeType = storeType;
  }

  public static OzoneManagerProtocolProtos.OzoneObj toProtobuf(HetuObj obj) {
    return OzoneManagerProtocolProtos.OzoneObj.newBuilder()
        .setResType(ObjectType.valueOf(obj.getResourceType().name()))
        .setStoreType(valueOf(obj.getStoreType().name()))
        .setPath(obj.getPath()).build();
  }

  public ResourceType getResourceType() {
    return resType;
  }

  @Override
  public String toString() {
    return "HetuObj{" +
        "resType=" + resType +
        ", storeType=" + storeType +
        ", path='" + getPath() + '\'' +
        '}';
  }

  public StoreType getStoreType() {
    return storeType;
  }

  public abstract String getDatabaseName();

  public abstract String getTableName();

  public abstract String getPartitionName();

  public abstract String getTabletName();

  /**
   * Get PrefixName.
   * A prefix name is like a key name under the bucket but
   * are mainly used for ACL for now and persisted into a separate prefix table.
   *
   * @return prefix name.
   */
  public abstract String getPrefixName();

  /**
   * Get full path of a tablet or prefix including database, table and partition.
   * @return full path of a key or prefix.
   */
  public abstract String getPath();

  /**
   * Ozone Objects supported for ACL.
   */
  public enum ResourceType {
    PREFIX(OzoneConsts.PREFIX),
    DATABASE(OzoneConsts.DATABASE),
    TABLE(OzoneConsts.TABLE),
    PARTITION(OzoneConsts.PARTITION),
    TABLET(OzoneConsts.TABLET);

    /**
     * String value for this Enum.
     */
    private final String value;

    @Override
    public String toString() {
      return value;
    }

    ResourceType(String resType) {
      value = resType;
    }
  }

  /**
   * Ozone Objects supported for ACL.
   */
  public enum StoreType {
    OZONE(OzoneConsts.OZONE),
    S3(OzoneConsts.S3);

    /**
     * String value for this Enum.
     */
    private final String value;

    @Override
    public String toString() {
      return value;
    }

    StoreType(String objType) {
      value = objType;
    }
  }

  public Map<String, String> toAuditMap() {
    Map<String, String> auditMap = new LinkedHashMap<>();
    auditMap.put(OzoneConsts.RESOURCE_TYPE, this.getResourceType().value);
    auditMap.put(OzoneConsts.STORAGE_TYPE, this.getStoreType().value);
    auditMap.put(OzoneConsts.DATABASE, this.getDatabaseName());
    auditMap.put(OzoneConsts.TABLE, this.getTableName());
    auditMap.put(OzoneConsts.PARTITION, this.getPartitionName());
    auditMap.put(OzoneConsts.TABLET, this.getTabletName());
    return auditMap;
  }

}
