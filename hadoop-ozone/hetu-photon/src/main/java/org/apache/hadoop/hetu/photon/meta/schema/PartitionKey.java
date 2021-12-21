package org.apache.hadoop.hetu.photon.meta.schema;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hetu.photon.meta.PartitionKeyType;
import org.apache.hadoop.hetu.photon.proto
        .PhotonSchemaProtocolProtos.SchemaProto;
import org.apache.hadoop.hetu.photon.proto
  .PhotonSchemaProtocolProtos.SchemaProto.PartitionKeyProto;

import java.util.List;
import java.util.Objects;

/**
 * Created by xiliu on 2021/8/13
 */
public class PartitionKey {
    private PartitionKeyType partitionKeyType;
    private List<String> fields;
    private int partitions;

    public PartitionKey(PartitionKeyType partitionKeyType,
                        List<String> fields) {
        Preconditions.checkNotNull(partitionKeyType);
        Preconditions.checkArgument(fields.size() > 0);
        this.partitionKeyType = partitionKeyType;
        this.fields = fields;
    }

    public PartitionKey(PartitionKeyType partitionKeyType,
                        List<String> fields,
                        int partitions) {
        Preconditions.checkNotNull(partitionKeyType);
        Preconditions.checkArgument(fields.size() > 0);
        if (partitionKeyType.equals(PartitionKeyType.HASH)) {
            Preconditions.checkArgument(partitions > 0);
        }
        this.partitionKeyType = partitionKeyType;
        this.fields = fields;
        this.partitions = partitions;
    }

    public PartitionKeyProto toProtobuf() {
        SchemaProto.PartitionKeyProto builder = SchemaProto.PartitionKeyProto.newBuilder()
                .setPartitionKeyType(partitionKeyType.toProto())
                .addAllFields(fields)
                .setPartitions(partitions).build();
        return builder;
    }

    public PartitionKeyType getPartitionKeyType() {
        return partitionKeyType;
    }

    public List<String> getFields() {
        return fields;
    }

    public int getPartitions() {
        return partitions;
    }

    public static PartitionKey fromProtobuf(SchemaProto.PartitionKeyProto partitionKeyProto) {
        if (partitionKeyProto.hasPartitions()) {
            return new PartitionKey(PartitionKeyType.valueOf(partitionKeyProto.getPartitionKeyType()),
                    partitionKeyProto.getFieldsList(), partitionKeyProto.getPartitions());
        } else {
            return new PartitionKey(PartitionKeyType.valueOf(partitionKeyProto.getPartitionKeyType()),
                    partitionKeyProto.getFieldsList());
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PartitionKey that = (PartitionKey) o;
        return partitions == that.partitions &&
                partitionKeyType == that.partitionKeyType &&
                fields.equals(that.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitionKeyType, fields, partitions);
    }

    @Override
    public String toString() {
        return "PartitionKey{" +
                "partitionKeyType=" + partitionKeyType +
                ", fields=" + fields +
                ", partitions=" + partitions +
                '}';
    }
}