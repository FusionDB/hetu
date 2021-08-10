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
package org.apache.hadoop.hetu.client;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.om.protocolPB.OmTransport;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TableInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PartitionInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TabletInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CommitTabletRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CommitTabletResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateTableRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateTableResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateTabletRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateTabletResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateDatabaseRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateDatabaseResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteDatabaseRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteDatabaseResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.InfoTableRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.InfoTableResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreatePartitionRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreatePartitionResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.InfoPartitionRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.InfoPartitionResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TabletArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TabletLocationList;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LookupTabletRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LookupTabletResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse.Builder;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ServiceListRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ServiceListResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DatabaseInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetDatabaseResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetDatabaseRequest;


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * OM transport for testing with in-memory state.
 */
public class MockOmTransport implements OmTransport {

  private final MockTabletAllocator tabletAllocator;
  //databaseName -> databaseInfo
  private Map<String, DatabaseInfo> databases = new HashMap<>();
  //databaseName -> tableName -> tableInfo
  private Map<String, Map<String, TableInfo>> tables = new HashMap<>();

  //databaseName -> tableName -> partitionName -> partitions
  private Map<String, Map<String, Map<String, PartitionInfo>>> partitions =
          new HashMap<>();

  //databaseName -> tableName -> partitionName -> tabletName -> tablets
  private Map<String, Map<String, Map<String, Map<String, TabletInfo>>>> openTablets =
      new HashMap<>();
  //databaseName -> tableName -> partitionName -> tabletName -> tablets
  private Map<String, Map<String, Map<String, Map<String, TabletInfo>>>> tablets =
          new HashMap<>();

  public MockOmTransport(MockTabletAllocator allocator) {
    this.tabletAllocator = allocator;
  }

  public MockOmTransport() {
    this.tabletAllocator = new SinglePipelineTabletAllocator();
  }

  @Override
  public OMResponse submitRequest(OMRequest payload) throws IOException {
    switch (payload.getCmdType()) {
    case CreateDatabase:
      return response(payload,
          r -> r.setCreateDatabaseResponse(
              createDatabase(payload.getCreateDatabaseRequest())));
    case GetDatabase:
      return response(payload,
          r -> r.setGetDatabaseResponse(
              getDatabase(payload.getGetDatabaseRequest())));
    case DeleteDatabase:
      return response(payload,
          r -> r.setDeleteDatabaseResponse(
              deleteDatabase(payload.getDeleteDatabaseRequest())));
    case CreateTable:
      return response(payload,
          r -> r.setCreateTableResponse(
              createTable(payload.getCreateTableRequest())));
    case InfoTable:
      return response(payload,
          r -> r.setInfoTableResponse(
              infoTable(payload.getInfoTableRequest())));
    case InfoPartition:
      return response(payload,
          r -> r.setInfoPartitionResponse(
              infoPartition(payload.getInfoPartitionRequest())));
    case CreatePartition:
      return response(payload,
          r -> r.setCreatePartitionResponse(
              createPartition(payload.getCreatePartitionRequest())));
    case CreateTablet:
      return response(payload,
          r -> r.setCreateTabletResponse(
              createTablet(payload.getCreateTabletRequest())));
    case CommitTablet:
      return response(payload,
          r -> r.setCommitTabletResponse(
              commitTablet(payload.getCommitTabletRequest())));
    case LookupTablet:
      return response(payload,
          r -> r.setLookupTabletResponse(
              lookupTablet(payload.getLookupTabletRequest())));
    case ServiceList:
      return response(payload,
          r -> r.setServiceListResponse(
              serviceList(payload.getServiceListRequest())));
    default:
      throw new IllegalArgumentException(
          "Mock version of om call " + payload.getCmdType()
              + " is not yet implemented");
    }
  }

  private DeleteDatabaseResponse deleteDatabase(
      DeleteDatabaseRequest deleteDatabaseRequest) {
    databases.remove(deleteDatabaseRequest.getName());
    return DeleteDatabaseResponse.newBuilder()
        .build();
  }

  private LookupTabletResponse lookupTablet(LookupTabletRequest lookupTabletRequest) {
    final TabletArgs tabletArgs = lookupTabletRequest.getTabletArgs();
    return LookupTabletResponse.newBuilder()
        .setTabletInfo(
            tablets.get(tabletArgs.getDatabaseName())
                .get(tabletArgs.getTableName())
                .get(tabletArgs.getPartitionName())
                .get(tabletArgs.getTabletName()))
        .build();
  }

  private CommitTabletResponse commitTablet(CommitTabletRequest commitTabletRequest) {
    final TabletArgs tabletArgs = commitTabletRequest.getTabletArgs();
    final TabletInfo remove =
        openTablets.get(tabletArgs.getDatabaseName()).get(tabletArgs.getTableName())
            .get(tabletArgs.getPartitionName())
            .remove(tabletArgs.getTabletName());
    tablets.get(tabletArgs.getDatabaseName()).get(tabletArgs.getTableName())
        .get(tabletArgs.getPartitionName())
        .put(tabletArgs.getTabletName(), remove);
    return CommitTabletResponse.newBuilder()
        .build();
  }

  private CreateTabletResponse createTablet(CreateTabletRequest createTabletRequest) {
    final TabletArgs tabletArgs = createTabletRequest.getTabletArgs();
    final long now = System.currentTimeMillis();
    final TabletInfo tabletInfo = TabletInfo.newBuilder()
        .setDatabaseName(tabletArgs.getDatabaseName())
        .setTableName(tabletArgs.getTableName())
        .setPartitionName(tabletArgs.getPartitionName())
        .setTabletName(tabletArgs.getTabletName())
        .setCreationTime(now)
        .setModificationTime(now)
        .setType(tabletArgs.getType())
        .setFactor(tabletArgs.getFactor())
        .setDataSize(tabletArgs.getDataSize())
        .setLatestVersion(0L)
        .addTabletLocationList(TabletLocationList.newBuilder()
            .addAllTabletLocation(
                tabletAllocator.allocateTablet(createTabletRequest.getTabletArgs()))
            .build())
        .build();
    openTablets.get(tabletInfo.getDatabaseName()).get(tabletInfo.getTableName())
        .get(tabletInfo.getPartitionName())
        .put(tabletInfo.getTabletName(), tabletInfo);
    return CreateTabletResponse.newBuilder()
        .setOpenVersion(0L)
        .setTabletInfo(tabletInfo)
        .build();
  }

  private InfoTableResponse infoTable(InfoTableRequest infoTableRequest) {
    return InfoTableResponse.newBuilder()
        .setTableInfo(tables.get(infoTableRequest.getDatabaseName())
            .get(infoTableRequest.getTableName()))
        .build();
  }

  private GetDatabaseResponse getDatabase(GetDatabaseRequest getDatabaseRequest) {
    final DatabaseInfo databaseInfo =
            databases.get(getDatabaseRequest.getName());
    if (databaseInfo == null) {
      throw new MockOmException(Status.DATABASE_NOT_FOUND);
    }
    return GetDatabaseResponse.newBuilder()
        .setDatabaseInfo(databaseInfo)
        .build();
  }

  private CreateDatabaseResponse createDatabase(
      CreateDatabaseRequest createDatabaseRequest) {
    databases.put(createDatabaseRequest.getDatabaseInfo().getName(),
        createDatabaseRequest.getDatabaseInfo());
    tables
        .put(createDatabaseRequest.getDatabaseInfo().getName(), new HashMap<>());
    partitions
        .put(createDatabaseRequest.getDatabaseInfo().getName(), new HashMap<>());
    openTablets
        .put(createDatabaseRequest.getDatabaseInfo().getName(), new HashMap<>());
    tablets
        .put(createDatabaseRequest.getDatabaseInfo().getName(), new HashMap<>());
    return CreateDatabaseResponse.newBuilder()
        .build();
  }

  private ServiceListResponse serviceList(
      ServiceListRequest serviceListRequest) {
    return ServiceListResponse.newBuilder()
        .build();
  }

  private OMResponse response(OMRequest payload,
      Function<Builder, Builder> function) {
    Builder builder = OMResponse.newBuilder();
    try {
      builder = function.apply(builder);
      builder.setSuccess(true);
      builder.setStatus(Status.OK);
    } catch (MockOmException e) {
      builder.setSuccess(false);
      builder.setStatus(e.getStatus());
    }

    builder.setCmdType(payload.getCmdType());
    return builder.build();
  }

  private CreateTableResponse createTable(
      CreateTableRequest createTableRequest) {
    final TableInfo tableInfo =
        TableInfo.newBuilder(createTableRequest.getTableInfo())
            .setCreationTime(System.currentTimeMillis())
            .build();

    tables.get(tableInfo.getDatabaseName())
        .put(tableInfo.getTableName(), tableInfo);
    partitions.get(tableInfo.getDatabaseName())
        .put(tableInfo.getTableName(), new HashMap<>());
    openTablets.get(tableInfo.getDatabaseName())
        .put(tableInfo.getTableName(), new HashMap<>());
    tablets.get(tableInfo.getDatabaseName())
        .put(tableInfo.getTableName(), new HashMap<>());
    return CreateTableResponse.newBuilder().build();
  }

  private CreatePartitionResponse createPartition(
          CreatePartitionRequest createPartitionRequest) {
    final PartitionInfo partitionInfo =
          PartitionInfo.newBuilder(createPartitionRequest.getPartitionInfo())
                    .setCreationTime(System.currentTimeMillis())
                    .build();

    partitions.get(partitionInfo.getDatabaseName())
            .get(partitionInfo.getTableName())
            .put(partitionInfo.getPartitionName(), partitionInfo);
    openTablets.get(partitionInfo.getDatabaseName())
            .get(partitionInfo.getTableName())
            .put(partitionInfo.getPartitionName(), new HashMap<>());
    tablets.get(partitionInfo.getDatabaseName())
            .get(partitionInfo.getTableName())
            .put(partitionInfo.getPartitionName(), new HashMap<>());
    return CreatePartitionResponse.newBuilder().build();
  }

  private InfoPartitionResponse infoPartition(InfoPartitionRequest infoPartitionRequest) {
    return InfoPartitionResponse.newBuilder()
            .setPartitionInfo(partitions.get(infoPartitionRequest.getDatabaseName())
                    .get(infoPartitionRequest.getTableName()).get(infoPartitionRequest.getPartitionName()))
            .build();
  }

  @Override
  public Text getDelegationTokenService() {
    return null;
  }

  @Override
  public void close() throws IOException {

  }

  /**
   * Error from mock OM API.
   */
  public static class MockOmException extends RuntimeException {

    private Status status;

    public MockOmException(
        Status status) {
      this.status = status;
    }

    public Status getStatus() {
      return status;
    }
  }

}
