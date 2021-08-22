package org.apache.hadoop.hetu.photon.meta.table;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hetu.photon.meta.RuleType;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SchemaProto;

import java.util.List;
import java.util.Objects;

/**
 * Created by xiliu on 2021/8/13
 */
public class DistributedKey {
    private RuleType distributedKeyRuleType;
    private List<String> fields;

    public DistributedKey(RuleType distributedKeyRuleType,
                          List<String> fields) {
        Preconditions.checkNotNull(distributedKeyRuleType);
        Preconditions.checkArgument(fields.size() > 0);
        this.distributedKeyRuleType = distributedKeyRuleType;
       this.fields = fields;
    }

    public SchemaProto.DistributedKeyProto toProtobuf() {
        SchemaProto.DistributedKeyProto builder = SchemaProto.DistributedKeyProto.newBuilder()
        .setDistributedKeyType(distributedKeyRuleType.toProto())
        .addAllFields(fields).build();
        return builder;
    }

    public RuleType getDistributedKeyRuleType() {
        return distributedKeyRuleType;
    }

    public List<String> getFields() {
        return fields;
    }

    public static DistributedKey fromProtobuf(SchemaProto.DistributedKeyProto distributedKeyProto) {
        return new DistributedKey(RuleType.valueOf(distributedKeyProto.getDistributedKeyType()),
                distributedKeyProto.getFieldsList());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DistributedKey that = (DistributedKey) o;
        return distributedKeyRuleType == that.distributedKeyRuleType &&
                fields.equals(that.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(distributedKeyRuleType, fields);
    }

    @Override
    public String toString() {
        return "DistributedKey{" +
                "distributedKeyType=" + distributedKeyRuleType +
                ", fields=" + fields +
                '}';
    }
}