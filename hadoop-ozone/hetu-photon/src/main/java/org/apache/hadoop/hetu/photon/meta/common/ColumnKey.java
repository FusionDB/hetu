package org.apache.hadoop.hetu.photon.meta.common;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hetu.photon.proto
   .PhotonSchemaProtocolProtos.SchemaProto.ColumnKeyProto;

import java.util.List;
import java.util.Objects;

/**
 * Created by xiliu on 2021/4/6
 */
public class ColumnKey {
    private ColumnKeyType columnKeyType;
    private List<String> fields;

    public ColumnKey(ColumnKeyType columnKeyType, List<String> fields) {
        Preconditions.checkNotNull(columnKeyType);
        Preconditions.checkArgument(fields.size() > 0);
        this.columnKeyType = columnKeyType;
        this.fields = fields;
    }

    public ColumnKeyProto toProtobuf() {
        ColumnKeyProto.Builder columnKeyProto = ColumnKeyProto.newBuilder()
                .setColumnKeyType(columnKeyType.toProto())
                .addAllFields(fields);
        return columnKeyProto.build();
    }

    public ColumnKeyType getColumnKeyType() {
        return columnKeyType;
    }

    public List<String> getFields() {
        return fields;
    }

    public static ColumnKey fromProtobuf(ColumnKeyProto columnKeyProto) {
        return new ColumnKey(ColumnKeyType.valueOf(columnKeyProto.getColumnKeyType()), columnKeyProto.getFieldsList());
    }

    @Override
    public String toString() {
        return "ColumnKey{" +
                "columnKeyType=" + columnKeyType +
                ", fields=" + fields +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ColumnKey columnKey = (ColumnKey) o;
        return columnKeyType == columnKey.columnKeyType &&
                Objects.equals(fields, columnKey.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnKeyType, fields);
    }
}
