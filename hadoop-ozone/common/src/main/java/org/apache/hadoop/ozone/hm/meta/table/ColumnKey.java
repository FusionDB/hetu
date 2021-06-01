package org.apache.hadoop.ozone.hm.meta.table;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ColumnSchemaProto.ColumnKeyProto;

/**
 * Created by xiliu on 2021/4/6
 */
public enum ColumnKey {
    PRIMARY_KEY,
    UNIQUE_KEY,
    AGGREGATE_KEY,
    NO_KEY;

    public ColumnKeyProto toProto() {
        switch (this) {
            case PRIMARY_KEY:
                return ColumnKeyProto.PRIMARY_KEY;
            case UNIQUE_KEY:
                return ColumnKeyProto.UNIQUE_KEY;
            case AGGREGATE_KEY:
                return ColumnKeyProto.AGGREGATE_KEY;
            case NO_KEY:
                return ColumnKeyProto.NO_KEY;
            default:
                throw new IllegalStateException(
                        "BUG: ColumnKey not found, key =" + this);
        }
    }

    public static ColumnKey valueOf(ColumnKeyProto key) {
        switch (key) {
            case PRIMARY_KEY:
                return PRIMARY_KEY;
            case UNIQUE_KEY:
                return UNIQUE_KEY;
            case AGGREGATE_KEY:
                return AGGREGATE_KEY;
            case NO_KEY:
                return NO_KEY;
            default:
                throw new IllegalStateException(
                        "BUG: ColumnKeyProto not found, key=" + key);
        }
    }
}
