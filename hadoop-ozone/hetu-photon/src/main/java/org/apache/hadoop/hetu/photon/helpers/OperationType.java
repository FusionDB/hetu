package org.apache.hadoop.hetu.photon.helpers;

import org.apache.hadoop.hetu.photon.proto.HetuPhotonProtos
        .OperationProto.OperationTypeProto;

/**
 * Created by xiliu on 2021/8/16
 */
public enum OperationType {
    INSERT,
    UPDATE,
    DELETE,
    UPSERT,
    SCAN_QUERY;

    public OperationTypeProto toProto() {
        switch (this) {
            case INSERT:
                return OperationTypeProto.INSERT;
            case UPDATE:
                return OperationTypeProto.UPDATE;
            case DELETE:
                return OperationTypeProto.DELETE;
            case UPSERT:
                return OperationTypeProto.UPSERT;
            case SCAN_QUERY:
                return OperationTypeProto.SCAN_QUERY;
            default:
                throw new IllegalStateException(
                        "BUG: Operation Type not found, type = " + this);
        }
    }

    public static OperationType valueOf(OperationTypeProto operationType) {
        switch (operationType) {
            case INSERT:
                return INSERT;
            case UPDATE:
                return UPDATE;
            case DELETE:
                return DELETE;
            case UPSERT:
                return UPSERT;
            case SCAN_QUERY:
                return SCAN_QUERY;
            default:
                throw new IllegalStateException(
                        "BUG: Operation TypeProto not found, type = " + operationType);
        }
    }
}
