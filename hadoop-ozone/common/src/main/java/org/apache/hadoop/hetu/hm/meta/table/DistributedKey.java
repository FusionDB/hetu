package org.apache.hadoop.hetu.hm.meta.table;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hetu.hm.Type;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SchemaProto;

import java.util.List;
import java.util.Objects;

/**
 * Created by xiliu on 2021/8/13
 */
public class DistributedKey {
    private Type distributedKeyType;
    private List<String> fields;

    public DistributedKey(Type distributedKeyType,
                        List<String> fields) {
        Preconditions.checkNotNull(distributedKeyType);
        Preconditions.checkArgument(fields.size() > 0);
        this.distributedKeyType = distributedKeyType;
       this.fields = fields;
    }

    public SchemaProto.DistributedKeyProto toProtobuf() {
        SchemaProto.DistributedKeyProto builder = SchemaProto.DistributedKeyProto.newBuilder()
        .setDistributedKeyType(distributedKeyType.toProto())
        .addAllFields(fields).build();
        return builder;
    }

    public Type getDistributedKeyType() {
        return distributedKeyType;
    }

    public List<String> getFields() {
        return fields;
    }

    public static DistributedKey fromProtobuf(SchemaProto.DistributedKeyProto distributedKeyProto) {
        return new DistributedKey(Type.valueOf(distributedKeyProto.getDistributedKeyType()),
                distributedKeyProto.getFieldsList());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DistributedKey that = (DistributedKey) o;
        return distributedKeyType == that.distributedKeyType &&
                fields.equals(that.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(distributedKeyType, fields);
    }

    @Override
    public String toString() {
        return "DistributedKey{" +
                "distributedKeyType=" + distributedKeyType +
                ", fields=" + fields +
                '}';
    }
}