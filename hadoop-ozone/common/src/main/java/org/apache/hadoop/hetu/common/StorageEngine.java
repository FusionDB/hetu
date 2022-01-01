package org.apache.hadoop.hetu.common;


import org.apache.hadoop.ozone.protocol.proto
   .OzoneManagerProtocolProtos.TableInfo.StorageEngineProto;

/**
 * Created by xiliu on 2021/8/10
 */
public enum StorageEngine {
    LSTORE,
    CSTORE;

    public StorageEngineProto toProto() {
        switch (this) {
            case LSTORE:
                return StorageEngineProto.LSTORE;
            case CSTORE:
                return StorageEngineProto.CSTORE;
            default:
                throw new IllegalStateException(
                        "BUG: StorageEngine not found, engine type = " + this);
        }
    }

    public static StorageEngine valueOf(StorageEngineProto key) {
        switch (key) {
            case LSTORE:
                return LSTORE;
            case CSTORE:
                return CSTORE;
            default:
                throw new IllegalStateException(
                        "BUG: StorageEngineProto not found, engine type = " + key);
        }
    }
}
