package org.apache.hadoop.hetu.photon.meta.table;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hetu.photon.meta.DistributedKeyType;
import org.apache.hadoop.hetu.photon.meta.PartitionKeyType;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SchemaProto;

import java.util.List;
import java.util.Objects;

/**
 * Created by xiliu on 2021/8/13
 */
public class DistributedKey {
    private DistributedKeyType distributedKeyType;
    private List<String> fields;
    private int buckets;

    public DistributedKey(DistributedKeyType distributedKeyType,
                          List<String> fields,
                          int buckets) {
        Preconditions.checkNotNull(distributedKeyType);
        Preconditions.checkArgument(fields.size() > 0);
        Preconditions.checkArgument(buckets > 0);
        this.distributedKeyType = distributedKeyType;
        this.fields = fields;
        this.buckets = buckets;
    }

    public SchemaProto.DistributedKeyProto toProtobuf() {
        SchemaProto.DistributedKeyProto builder = SchemaProto.DistributedKeyProto.newBuilder()
        .setDistributedKeyType(distributedKeyType.toProto())
        .addAllFields(fields)
        .setBuckets(buckets)
        .build();
        return builder;
    }

    public DistributedKeyType getDistributedKeyType() {
        return distributedKeyType;
    }

    public List<String> getFields() {
        return fields;
    }

    public int getBuckets() {
        return buckets;
    }

    public static DistributedKey fromProtobuf(SchemaProto.DistributedKeyProto distributedKeyProto) {
        return new DistributedKey(DistributedKeyType.valueOf(distributedKeyProto.getDistributedKeyType()),
                distributedKeyProto.getFieldsList(), distributedKeyProto.getBuckets());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DistributedKey that = (DistributedKey) o;
        return buckets == that.buckets &&
                distributedKeyType == that.distributedKeyType &&
                fields.equals(that.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(distributedKeyType, fields, buckets);
    }

    @Override
    public String toString() {
        return "DistributedKey{" +
                "distributedKeyType=" + distributedKeyType +
                ", fields=" + fields +
                ", buckets=" + buckets +
                '}';
    }
}