package org.apache.hadoop.hetu.photon.meta.table;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hetu.photon.meta.RuleType;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SchemaProto;

import java.util.List;
import java.util.Objects;

/**
 * Created by xiliu on 2021/8/13
 */
public class PartitionKey {
    private RuleType partitionKeyRuleType;
    private List<String> fields;

    public PartitionKey(RuleType partitionKeyRuleType,
                        List<String> fields) {
       Preconditions.checkNotNull(partitionKeyRuleType);
       Preconditions.checkArgument(fields.size() > 0);
       this.partitionKeyRuleType = partitionKeyRuleType;
       this.fields = fields;
    }

    public SchemaProto.PartitionKeyProto toProtobuf() {
        SchemaProto.PartitionKeyProto builder = SchemaProto.PartitionKeyProto.newBuilder()
        .setPartitionKeyType(partitionKeyRuleType.toProto())
        .addAllFields(fields).build();
        return builder;
    }

    public RuleType getDistributedKeyType() {
        return partitionKeyRuleType;
    }

    public List<String> getFields() {
        return fields;
    }

    public static PartitionKey fromProtobuf(SchemaProto.PartitionKeyProto partitionKeyProto) {
        return new PartitionKey(RuleType.valueOf(partitionKeyProto.getPartitionKeyType()),
                partitionKeyProto.getFieldsList());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PartitionKey that = (PartitionKey) o;
        return partitionKeyRuleType == that.partitionKeyRuleType &&
                fields.equals(that.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitionKeyRuleType, fields);
    }

    @Override
    public String toString() {
        return "PartitionKey{" +
                "partitionKeyType=" + partitionKeyRuleType +
                ", fields=" + fields +
                '}';
    }
}