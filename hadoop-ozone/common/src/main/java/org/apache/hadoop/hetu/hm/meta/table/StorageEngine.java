package org.apache.hadoop.hetu.hm.meta.table;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;

/**
 * Created by xiliu on 2021/8/10
 */
public enum StorageEngine {
    LSTORE,
    CSTORE;

    public OzoneManagerProtocolProtos.TableInfo.StorageEngineProto toProto() {
        switch (this) {
            case LSTORE:
                return OzoneManagerProtocolProtos.TableInfo.StorageEngineProto.LSTORE;
            case CSTORE:
                return OzoneManagerProtocolProtos.TableInfo.StorageEngineProto.CSTORE;
            default:
                throw new IllegalStateException(
                        "BUG: StorageEngine not found, engine type = " + this);
        }
    }

    public static StorageEngine valueOf(OzoneManagerProtocolProtos.TableInfo.StorageEngineProto key) {
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
