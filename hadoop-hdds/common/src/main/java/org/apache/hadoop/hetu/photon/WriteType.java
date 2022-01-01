package org.apache.hadoop.hetu.photon;

import org.apache.hadoop.hdds.protocol.datanode.proto
        .ContainerProtos.WriteChunkRequestProto.WriteTypeProto;

/**
 * Created by xiliu on 2021/8/16
 */
public enum WriteType {
    INSERT,
    UPDATE,
    DELETE,
    UPSERT;

    public WriteTypeProto toProto() {
        switch (this) {
            case INSERT:
                return WriteTypeProto.INSERT;
            case UPDATE:
                return WriteTypeProto.UPDATE;
            case DELETE:
                return WriteTypeProto.DELETE;
            case UPSERT:
                return WriteTypeProto.UPSERT;
            default:
                throw new IllegalStateException(
                        "BUG: WriteType not found, type = " + this);
        }
    }

    public static WriteType valueOf(WriteTypeProto writeType) {
        switch (writeType) {
            case INSERT:
                return INSERT;
            case UPDATE:
                return UPDATE;
            case DELETE:
                return DELETE;
            case UPSERT:
                return UPSERT;
            default:
                throw new IllegalStateException(
                        "BUG: WriteTypeProto not found, type = " + writeType);
        }
    }
}
