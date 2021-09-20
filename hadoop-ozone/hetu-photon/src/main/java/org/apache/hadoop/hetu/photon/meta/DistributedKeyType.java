package org.apache.hadoop.hetu.photon.meta;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SchemaProto.TypeProto;

/**
 * Created by xiliu on 2021/8/10
 */
public enum DistributedKeyType {
    HASH,
    RANGE,
    LIST;

    public TypeProto toProto() {
        switch (this) {
            case HASH:
                return TypeProto.HASH;
            case RANGE:
                return TypeProto.RANGE;
            case LIST:
                return TypeProto.LIST;
            default:
                throw new IllegalStateException(
                        "BUG: Type not found, type = " + this);
        }
    }

    public static DistributedKeyType valueOf(TypeProto type) {
        switch (type) {
            case HASH:
                return HASH;
            case RANGE:
                return RANGE;
            case LIST:
                return LIST;
            default:
                throw new IllegalStateException(
                        "BUG: TypeProto not found, type = " + type);
        }
    }
}
