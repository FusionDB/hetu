package org.apache.hadoop.hetu.photon;

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadChunkRequestProto.ReadTypeProto;

/**
 * Created by xiliu on 2021/8/16
 */
public enum ReadType {
    QUERY,
    SCAN;

    public ReadTypeProto toProto() {
        switch (this) {
            case QUERY:
                return ReadTypeProto.QUERY;
            case SCAN:
                return ReadTypeProto.SCAN;
            default:
                throw new IllegalStateException(
                        "BUG: WriteType not found, type = " + this);
        }
    }

    public static ReadType valueOf(ReadTypeProto readTYpe) {
        switch (readTYpe) {
            case QUERY:
                return QUERY;
            case SCAN:
                return SCAN;
            default:
                throw new IllegalStateException(
                        "BUG: ReadTypeProto not found, type = " + readTYpe);
        }
    }
}
