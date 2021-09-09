package org.apache.hadoop.hetu.photon.operation.request;

import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.hetu.photon.operation.OperationType;
import org.apache.hadoop.hetu.photon.proto.HetuPhotonProtos.OperationRequestProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by xiliu on 2021/9/3
 */
public class OperationRequest {
    private static final Logger LOG =
            LoggerFactory.getLogger(OperationRequest.class);

    private OperationType operationType;
    private DeleteOperationRequest deleteOperationRequest;
    private InsertOperationRequest insertOperationRequest;
    private UpdateOperationRequest updateOperationRequest;
    private UpsertOperationRequest upsertOperationRequest;
    private ScanQueryOperationRequest scanQueryOperationRequest;

    public OperationRequest(OperationType operationType,
                            DeleteOperationRequest deleteOperationRequest,
                            InsertOperationRequest insertOperationRequest,
                            UpdateOperationRequest updateOperationRequest,
                            UpsertOperationRequest upsertOperationRequest,
                            ScanQueryOperationRequest scanQueryOperationRequest) {
        this.operationType = operationType;
        this.deleteOperationRequest = deleteOperationRequest;
        this.insertOperationRequest = insertOperationRequest;
        this.updateOperationRequest = updateOperationRequest;
        this.upsertOperationRequest = upsertOperationRequest;
        this.scanQueryOperationRequest = scanQueryOperationRequest;
    }

    public OperationType getOperationType() {
        return operationType;
    }

    public DeleteOperationRequest getDeleteOperationRequest() {
        return deleteOperationRequest;
    }

    public InsertOperationRequest getInsertOperationRequest() {
        return insertOperationRequest;
    }

    public UpdateOperationRequest getUpdateOperationRequest() {
        return updateOperationRequest;
    }

    public UpsertOperationRequest getUpsertOperationRequest() {
        return upsertOperationRequest;
    }

    public ScanQueryOperationRequest getScanQueryOperationRequest() {
        return scanQueryOperationRequest;
    }

    /**
     * Returns new builder class that builds a OmTableArgs.
     * @return Builder
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for ColumnSchema.
     */
    public static class Builder {
        private OperationType operationType;
        private DeleteOperationRequest deleteOperationRequest;
        private InsertOperationRequest insertOperationRequest;
        private UpdateOperationRequest updateOperationRequest;
        private UpsertOperationRequest upsertOperationRequest;
        private ScanQueryOperationRequest scanQueryOperationRequest;

        public Builder setOperationType(OperationType operationType) {
            this.operationType = operationType;
            return this;
        }

        public Builder setDeleteOperationRequest(DeleteOperationRequest deleteOperationRequest) {
            this.deleteOperationRequest = deleteOperationRequest;
            return this;
        }

        public Builder setInsertOperationRequest(InsertOperationRequest insertOperationRequest) {
            this.insertOperationRequest = insertOperationRequest;
            return this;
        }

        public Builder setUpdateOperationRequest(UpdateOperationRequest updateOperationRequest) {
            this.updateOperationRequest = updateOperationRequest;
            return this;
        }

        public Builder setUpsertOperationRequest(UpsertOperationRequest upsertOperationRequest) {
            this.upsertOperationRequest = upsertOperationRequest;
            return this;
        }

        public Builder setScanQueryOperationRequest(ScanQueryOperationRequest scanQueryOperationRequest) {
            this.scanQueryOperationRequest = scanQueryOperationRequest;
            return this;
        }

        public OperationRequest build() {
            Preconditions.checkNotNull(operationType);
            switch (operationType) {
                case SCAN_QUERY:
                    Preconditions.checkNotNull(scanQueryOperationRequest);
                    break;
                case UPSERT:
                    Preconditions.checkNotNull(upsertOperationRequest);
                    break;
                case DELETE:
                    Preconditions.checkNotNull(deleteOperationRequest);
                    break;
                case INSERT:
                    Preconditions.checkNotNull(insertOperationRequest);
                    break;
                case UPDATE:
                    Preconditions.checkNotNull(updateOperationRequest);
                    break;
                default:
                    throw new RuntimeException("Unsupported operation request type: " + operationType);
            }
            return new OperationRequest(operationType,
                    deleteOperationRequest,
                    insertOperationRequest,
                    updateOperationRequest,
                    upsertOperationRequest,
                    scanQueryOperationRequest);
        }
    }

    public OperationRequestProto toProto() {
        OperationRequestProto.Builder operationRequest = OperationRequestProto.newBuilder()
                .setOperationType(operationType.toProto());
        switch (operationType) {
            case SCAN_QUERY:
                operationRequest.setScanQueryOperation(scanQueryOperationRequest.toProto());
                break;
            case UPSERT:
                operationRequest.setUpsertOperation(upsertOperationRequest.toProto());
                break;
            case DELETE:
                operationRequest.setDeleteOperation(deleteOperationRequest.toProto());
                break;
            case INSERT:
                operationRequest.setInsertOperation(insertOperationRequest.toProto());
                break;
            case UPDATE:
                operationRequest.setUpdateOperation(updateOperationRequest.toProto());
                break;
            default:
                throw new RuntimeException("Unsupported operation request type: " + operationType);
        }
        return operationRequest.build();
    }

    public static OperationRequest fromProtobuf(OperationRequestProto operationRequest) {
        return new OperationRequest(OperationType.valueOf(operationRequest.getOperationType()),
                DeleteOperationRequest.fromProtobuf(operationRequest.getDeleteOperation()),
                InsertOperationRequest.fromProtobuf(operationRequest.getInsertOperation()),
                UpdateOperationRequest.fromProtobuf(operationRequest.getUpdateOperation()),
                UpsertOperationRequest.fromProtobuf(operationRequest.getUpsertOperation()),
                ScanQueryOperationRequest.fromProtobuf(operationRequest.getScanQueryOperation()));
    }

    public static byte[] toPersistedFormat(OperationRequest object) throws IOException {
        Preconditions
                .checkNotNull(object, "Null object can't be converted to byte array.");
        return object.toProto().toByteArray();
    }

    public static OperationRequest fromPersistedFormat(byte[] rawData) throws IOException {
        Preconditions
                .checkNotNull(rawData,
                        "Null byte array can't converted to real object.");
        try {
            return OperationRequest.fromProtobuf(OperationRequestProto.parseFrom(rawData));
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException(
                    "Can't encode the the raw data from the byte array", e);
        }
    }

    public OperationRequest copyObject(OperationRequest operation) {
        return operation.copyObject();
    }

    private OperationRequest copyObject() {
        return OperationRequest.newBuilder()
                .setOperationType(operationType)
                .setUpdateOperationRequest(updateOperationRequest)
                .setInsertOperationRequest(insertOperationRequest)
                .setDeleteOperationRequest(deleteOperationRequest)
                .setUpsertOperationRequest(upsertOperationRequest)
                .setScanQueryOperationRequest(scanQueryOperationRequest)
                .build();
    }

    @Override
    public String toString() {
        return "OperationRequest{" +
                "operationType=" + operationType +
                ", deleteOperationRequest=" + deleteOperationRequest +
                ", insertOperationRequest=" + insertOperationRequest +
                ", updateOperationRequest=" + updateOperationRequest +
                ", upsertOperationRequest=" + upsertOperationRequest +
                ", scanQueryOperationRequest=" + scanQueryOperationRequest +
                '}';
    }
}
