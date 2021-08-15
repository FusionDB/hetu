package org.apache.hadoop.hetu.hm.meta.table;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hetu.hm.Type;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SchemaProto;

import java.util.List;
import java.util.Objects;

/**
 * Created by xiliu on 2021/8/13
 */
public class PartitionKey {
    private Type partitionKeyType;
    private List<String> fields;

    public PartitionKey(Type partitionKeyType,
                        List<String> fields) {
       Preconditions.checkNotNull(partitionKeyType);
       Preconditions.checkArgument(fields.size() > 0);
       this.partitionKeyType = partitionKeyType;
       this.fields = fields;
    }

    public SchemaProto.PartitionKeyProto toProtobuf() {
        SchemaProto.PartitionKeyProto builder = SchemaProto.PartitionKeyProto.newBuilder()
        .setPartitionKeyType(partitionKeyType.toProto())
        .addAllFields(fields).build();
        return builder;
    }

    public Type getDistributedKeyType() {
        return partitionKeyType;
    }

    public List<String> getFields() {
        return fields;
    }

    public static PartitionKey fromProtobuf(SchemaProto.PartitionKeyProto partitionKeyProto) {
        return new PartitionKey(Type.valueOf(partitionKeyProto.getPartitionKeyType()),
                partitionKeyProto.getFieldsList());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PartitionKey that = (PartitionKey) o;
        return partitionKeyType == that.partitionKeyType &&
                fields.equals(that.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitionKeyType, fields);
    }

    @Override
    public String toString() {
        return "PartitionKey{" +
                "partitionKeyType=" + partitionKeyType +
                ", fields=" + fields +
                '}';
    }
}