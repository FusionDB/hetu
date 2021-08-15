package org.apache.hadoop.hetu.hm.meta.table;

import com.google.common.base.Preconditions;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * Created by xiliu on 2021/8/13
 */
public class Schema {
    private List<ColumnSchema> columnSchemaList;
    private ColumnKey columnKey;
    private DistributedKey distributedKey;
    private PartitionKey partitionKey;

    public Schema(List<ColumnSchema> columnSchemaList,
                  ColumnKey columnKey,
                  DistributedKey distributedKey,
                  PartitionKey partitionKey) {
        Preconditions.checkArgument(columnSchemaList.size() > 0);
        this.columnSchemaList = columnSchemaList;
        this.columnKey = columnKey;
        this.distributedKey = distributedKey;
        this.partitionKey = partitionKey;
    }

    public OzoneManagerProtocolProtos.SchemaProto toProtobuf() {
        List<OzoneManagerProtocolProtos.ColumnSchemaProto> columnSchemaProtos = columnSchemaList.stream()
                .map(columnSchema -> columnSchema.toProtobuf())
                .collect(Collectors.toList());

        OzoneManagerProtocolProtos.SchemaProto builder = OzoneManagerProtocolProtos.SchemaProto.newBuilder()
                .addAllColumns(columnSchemaProtos)
                .setColumnKey(columnKey.toProtobuf())
                .setDistributedKey(distributedKey.toProtobuf())
                .setPartitionKey(partitionKey.toProtobuf())
                .build();
        return builder;
    }

    public List<ColumnSchema> getColumnSchemaList() {
        return columnSchemaList;
    }

    public ColumnKey getColumnKey() {
        return columnKey;
    }

    public DistributedKey getDistributedKey() {
        return distributedKey;
    }

    public PartitionKey getPartitionKey() {
        return partitionKey;
    }

    public static Schema fromProtobuf(OzoneManagerProtocolProtos.SchemaProto schemaProto) {
        List<ColumnSchema> columnSchemaList = schemaProto.getColumnsList().stream()
                .map(columnSchemaProto -> ColumnSchema.fromProtobuf(columnSchemaProto))
                .collect(Collectors.toList());
        return new Schema(columnSchemaList,
                ColumnKey.fromProtobuf(schemaProto.getColumnKey()),
                DistributedKey.fromProtobuf(schemaProto.getDistributedKey()),
                PartitionKey.fromProtobuf(schemaProto.getPartitionKey()));
    }

    @Override
    public String toString() {
        return "Schema{" +
                "columnSchemaList=" + columnSchemaList +
                ", columnKey=" + columnKey +
                ", distributedKey=" + distributedKey +
                ", partitionKey=" + partitionKey +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Schema schema = (Schema) o;
        return columnSchemaList.equals(schema.columnSchemaList) &&
                columnKey.equals(schema.columnKey) &&
                distributedKey.equals(schema.distributedKey) &&
                partitionKey.equals(schema.partitionKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnSchemaList, columnKey, distributedKey, partitionKey);
    }
}
