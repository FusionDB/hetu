package org.apache.hadoop.hetu.photon.meta.common;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SchemaProto.ColumnKeyTypeProto;

/**
 * Created by xiliu on 2021/6/23
 */
public enum ColumnKeyType {
    PRIMARY_KEY,
    UNIQUE_KEY,
    AGGREGATE_KEY,
    NO_KEY;

    public ColumnKeyTypeProto toProto() {
        switch (this) {
            case PRIMARY_KEY:
                return ColumnKeyTypeProto.PRIMARY_KEY;
            case UNIQUE_KEY:
                return ColumnKeyTypeProto.UNIQUE_KEY;
            case AGGREGATE_KEY:
                return ColumnKeyTypeProto.AGGREGATE_KEY;
            case NO_KEY:
                return ColumnKeyTypeProto.NO_KEY;
            default:
                throw new IllegalStateException(
                        "BUG: ColumnKeyType not found, key type = " + this);
        }
    }

    public static ColumnKeyType valueOf(ColumnKeyTypeProto key) {
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
                        "BUG: ColumnKeyTypeProto not found, key type = " + key);
        }
    }
}
