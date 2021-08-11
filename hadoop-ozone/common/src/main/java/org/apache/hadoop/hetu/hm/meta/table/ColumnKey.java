package org.apache.hadoop.hetu.hm.meta.table;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TableInfo.ColumnKeyProto;

import java.util.List;

/**
 * Created by xiliu on 2021/4/6
 */
public class ColumnKey {
    private ColumnKeyType columnKeyType;
    private List<String> fields;

    public ColumnKey(ColumnKeyType columnKeyType, List<String> fields) {
        this.columnKeyType = columnKeyType;
        this.fields = fields;
    }

    public ColumnKeyProto toProtobuf() {
        ColumnKeyProto.Builder columnKeyProto = ColumnKeyProto.newBuilder()
                .setColumnKeyType(columnKeyType.toProto())
                .addAllFields(fields);
        return columnKeyProto.build();
    }

    public static ColumnKey fromProtobuf(ColumnKeyProto columnKeyProto) {
        return new ColumnKey(ColumnKeyType.valueOf(columnKeyProto.getColumnKeyType()), columnKeyProto.getFieldsList());
    }
}
