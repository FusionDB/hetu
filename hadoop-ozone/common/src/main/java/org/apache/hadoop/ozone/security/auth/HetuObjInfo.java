/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.security.auth;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmTabletArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;

/**
 * Class representing an hetu object.
 * It can be a database with non-null databaseName (tableName=null & databaseName=null)
 * or a table with non-null databaseName and tableName (tableName=null)
 * or a key with non-null databaseName, tableName, partitionName and tablet name
 * (via getTabletName)
 * or a prefix with non-null databaseName, tableName, partitionName and prefix name
 * (via getPrefixName)
 */
public final class HetuObjInfo extends HetuObj {

  private final String databaseName;
  private final String tableName;
  private final String partitionName;
  private final String tabletName;

  /**
   *
   * @param resType
   * @param storeType
   * @param databaseName
   * @param tableName
   * @param partitionName
   * @param tableName - tabletName/PrefixTabletName
   */
  private HetuObjInfo(ResourceType resType, StoreType storeType,
                      String databaseName, String tableName, String partitionName,
                      String tabletName) {
    super(resType, storeType);
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.partitionName = partitionName;
    this.tabletName = tabletName;
  }

  @Override
  public String getPath() {
    switch (getResourceType()) {
    case DATABASE:
      return OZONE_URI_DELIMITER + getDatabaseName();
    case TABLE:
      return OZONE_URI_DELIMITER + getDatabaseName()
          + OZONE_URI_DELIMITER + getTableName();
    case PARTITION:
       return OZONE_URI_DELIMITER + getDatabaseName()
          + OZONE_URI_DELIMITER + getTableName()
          + OZONE_URI_DELIMITER + getPartitionName();
    case TABLET:
      return OZONE_URI_DELIMITER + getDatabaseName()
          + OZONE_URI_DELIMITER + getTableName()
          + OZONE_URI_DELIMITER + getPartitionName()
          + OZONE_URI_DELIMITER + getTabletName();
    case PREFIX:
      return OZONE_URI_DELIMITER + getDatabaseName()
          + OZONE_URI_DELIMITER + getTableName()
          + OZONE_URI_DELIMITER + getPartitionName()
          + OZONE_URI_DELIMITER + getPrefixName();
    default:
      throw new IllegalArgumentException("Unknown resource " +
        "type" + getResourceType());
    }
  }

  @Override
  public String getDatabaseName() {
    return databaseName;
  }

  @Override
  public String getTableName() {
    return tableName;
  }

  @Override
  public String getPartitionName() {
    return partitionName;
  }

  @Override
  public String getTabletName() {
    return tabletName;
  }

  @Override
  public String getPrefixName() {
    return tabletName;
  }


  public static HetuObjInfo fromProtobuf(OzoneManagerProtocolProtos.HetuObj
      proto) {
    Builder builder = new Builder()
        .setResType(ResourceType.valueOf(proto.getResType().name()))
        .setStoreType(StoreType.valueOf(proto.getStoreType().name()));
    String[] tokens = StringUtils.split(proto.getPath(),
        OZONE_URI_DELIMITER, 3);
    if(tokens == null) {
      throw new IllegalArgumentException("Unexpected path:" + proto.getPath());
    }
    // Set database name.
    switch (proto.getResType()) {
    case DATABASE:
      builder.setDatabaseName(tokens[0]);
      break;
    case TABLE:
      if (tokens.length < 2) {
        throw new IllegalArgumentException("Unexpected argument for " +
            "Ozone table. Path:" + proto.getPath());
      }
      builder.setDatabaseName(tokens[0]);
      builder.setTableName(tokens[1]);
      break;
    case PARTITION:
      if (tokens.length < 3) {
        throw new IllegalArgumentException("Unexpected argument for " +
                "Ozone partition. Path:" + proto.getPath());
      }
      builder.setDatabaseName(tokens[0]);
      builder.setTableName(tokens[1]);
      builder.setPartitionName(tokens[2]);
      break;
    case TABLET:
      if (tokens.length < 4) {
        throw new IllegalArgumentException("Unexpected argument for " +
            "Ozone tablet. Path:" + proto.getPath());
      }
      builder.setDatabaseName(tokens[0]);
      builder.setTableName(tokens[1]);
      builder.setPartitionName(tokens[2]);
      builder.setTabletName(tokens[3]);
      break;
    case PREFIX:
      if (tokens.length < 4) {
        throw new IllegalArgumentException("Unexpected argument for " +
            "Ozone Prefix. Path:" + proto.getPath());
      }
      builder.setDatabaseName(tokens[0]);
      builder.setTableName(tokens[1]);
      builder.setPartitionName(tokens[2]);
      builder.setPrefixName(tokens[3]);
      break;
    default:
      throw new IllegalArgumentException("Unexpected type for " +
          "Ozone tablet. Type:" + proto.getResType());
    }
    return builder.build();
  }

  /**
   * Inner builder class.
   */
  public static class Builder {

    private ResourceType resType;
    private StoreType storeType;
    private String databaseName;
    private String tableName;
    private String partitionName;
    private String tabletName;

    public static Builder newBuilder() {
      return new Builder();
    }

    public static Builder getBuilder(ResourceType resType,
        StoreType storeType, String database, String table,
        String partition, String tablet) {
      return Builder.newBuilder()
          .setResType(resType)
          .setStoreType(storeType)
          .setDatabaseName(database)
          .setTableName(table)
          .setPartitionName(partition)
          .setTabletName(tablet);
    }

    public static Builder fromTabletArgs(OmTabletArgs args) {
      return new Builder()
          .setDatabaseName(args.getDatabaseName())
          .setTableName(args.getTableName())
          .setPartitionName(args.getPartitionName())
          .setTabletName(args.getTabletName())
          .setResType(ResourceType.TABLET);
    }

    public Builder setResType(ResourceType res) {
      this.resType = res;
      return this;
    }

    public Builder setStoreType(StoreType store) {
      this.storeType = store;
      return this;
    }

    public Builder setDatabaseName(String database) {
      this.databaseName = database;
      return this;
    }

    public Builder setTableName(String table) {
      this.tableName = table;
      return this;
    }

    public Builder setPartitionName(String partition) {
      this.partitionName = partition;
      return this;
    }

    public Builder setTabletName(String tablet) {
      this.tabletName = tablet;
      return this;
    }

    public Builder setPrefixName(String prefix) {
      this.tabletName = prefix;
      return this;
    }

    public HetuObjInfo build() {
      return new HetuObjInfo(resType, storeType, databaseName, tableName, partitionName, tabletName);
    }
  }
}
