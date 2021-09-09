package org.apache.hadoop.hetu.photon.operation.response;

import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.hetu.photon.operation.OperationType;
import org.apache.hadoop.hetu.photon.proto.HetuPhotonProtos.OperationResponseProto;

import java.io.IOException;

/**
 * Created by xiliu on 2021/9/3
 */
public class OperationResponse {
    private OperationType operationType;
    private String message;

    private DeleteOperationResponse deleteOperationResponse;
    private InsertOperationResponse insertOperationResponse;
    private ScanQueryOperationResponse scanQueryOperationResponse;
    private UpdateOperationResponse updateOperationResponse;
    private UpsertOperationResponse upsertOperationResponse;

    public OperationResponse(OperationType operationType, String message,
                             DeleteOperationResponse deleteOperationResponse,
                             InsertOperationResponse insertOperationResponse,
                             ScanQueryOperationResponse scanQueryOperationResponse,
                             UpdateOperationResponse updateOperationResponse,
                             UpsertOperationResponse upsertOperationResponse) {
        this.operationType = operationType;
        this.message = message;
        this.deleteOperationResponse = deleteOperationResponse;
        this.insertOperationResponse = insertOperationResponse;
        this.scanQueryOperationResponse = scanQueryOperationResponse;
        this.updateOperationResponse = updateOperationResponse;
        this.upsertOperationResponse = upsertOperationResponse;
    }

    public OperationType getOperationType() {
        return operationType;
    }

    public String getMessage() {
        return message;
    }

    public DeleteOperationResponse getDeleteOperationResponse() {
        return deleteOperationResponse;
    }

    public InsertOperationResponse getInsertOperationResponse() {
        return insertOperationResponse;
    }

    public ScanQueryOperationResponse getScanQueryOperationResponse() {
        return scanQueryOperationResponse;
    }

    public UpdateOperationResponse getUpdateOperationResponse() {
        return updateOperationResponse;
    }

    public UpsertOperationResponse getUpsertOperationResponse() {
        return upsertOperationResponse;
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
        private String message;

        private DeleteOperationResponse deleteOperationResponse;
        private InsertOperationResponse insertOperationResponse;
        private ScanQueryOperationResponse scanQueryOperationResponse;
        private UpdateOperationResponse updateOperationResponse;
        private UpsertOperationResponse upsertOperationResponse;

        public Builder setOperationType(OperationType operationType) {
            this.operationType = operationType;
            return this;
        }

        public Builder setMessage(String message) {
            this.message = message;
            return this;
        }

        public Builder setDeleteOperationResponse(DeleteOperationResponse deleteOperationResponse) {
            this.deleteOperationResponse = deleteOperationResponse;
            return this;
        }

        public Builder setInsertOperationResponse(InsertOperationResponse insertOperationResponse) {
            this.insertOperationResponse = insertOperationResponse;
            return this;
        }

        public Builder setScanQueryOperationResponse(ScanQueryOperationResponse scanQueryOperationResponse) {
            this.scanQueryOperationResponse = scanQueryOperationResponse;
            return this;
        }

        public Builder setUpdateOperationResponse(UpdateOperationResponse updateOperationResponse) {
            this.updateOperationResponse = updateOperationResponse;
            return this;
        }

        public Builder setUpsertOperationResponse(UpsertOperationResponse upsertOperationResponse) {
            this.upsertOperationResponse = upsertOperationResponse;
            return this;
        }

        public OperationResponse build() {
            Preconditions.checkNotNull(operationType);
            switch (operationType) {
                case SCAN_QUERY:
                    Preconditions.checkNotNull(scanQueryOperationResponse);
                    break;
                case UPSERT:
                    Preconditions.checkNotNull(upsertOperationResponse);
                    break;
                case DELETE:
                    Preconditions.checkNotNull(deleteOperationResponse);
                    break;
                case INSERT:
                    Preconditions.checkNotNull(insertOperationResponse);
                    break;
                case UPDATE:
                    Preconditions.checkNotNull(updateOperationResponse);
                    break;
                default:
                    throw new RuntimeException("Unsupported operation response type: " + operationType);
            }
            return new OperationResponse(operationType,
                    message,
                    deleteOperationResponse,
                    insertOperationResponse,
                    scanQueryOperationResponse,
                    updateOperationResponse,
                    upsertOperationResponse);
        }
    }

    public OperationResponseProto toProto() {
        OperationResponseProto.Builder OperationResponse = OperationResponseProto.newBuilder()
                .setOperationType(operationType.toProto());
        switch (operationType) {
            case SCAN_QUERY:
                OperationResponse.setScanQueryOperation(scanQueryOperationResponse.toProto());
                break;
            case UPSERT:
                OperationResponse.setUpsertOperation(upsertOperationResponse.toProto());
                break;
            case DELETE:
                OperationResponse.setDeleteOperation(deleteOperationResponse.toProto());
                break;
            case INSERT:
                OperationResponse.setInsertOperation(insertOperationResponse.toProto());
                break;
            case UPDATE:
                OperationResponse.setUpdateOperation(updateOperationResponse.toProto());
                break;
            default:
                throw new RuntimeException("Unsupported operation request type: " + operationType);
        }
        return OperationResponse.build();
    }

    public static OperationResponse fromProtobuf(OperationResponseProto operationResponse) {
        return new OperationResponse(OperationType.valueOf(operationResponse.getOperationType()),
                operationResponse.getMessage(),
                DeleteOperationResponse.fromProtobuf(operationResponse.getDeleteOperation()),
                InsertOperationResponse.fromProtobuf(operationResponse.getInsertOperation()),
                ScanQueryOperationResponse.fromProtobuf(operationResponse.getScanQueryOperation()),
                UpdateOperationResponse.fromProtobuf(operationResponse.getUpdateOperation()),
                UpsertOperationResponse.fromProtobuf(operationResponse.getUpsertOperation()));
    }

    public static byte[] toPersistedFormat(OperationResponse object) throws IOException {
        Preconditions
                .checkNotNull(object, "Null object can't be converted to byte array.");
        return object.toProto().toByteArray();
    }

    public static OperationResponse fromPersistedFormat(byte[] rawData) throws IOException {
        Preconditions
                .checkNotNull(rawData,
                        "Null byte array can't converted to real object.");
        try {
            return OperationResponse.fromProtobuf(OperationResponseProto.parseFrom(rawData));
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException(
                    "Can't encode the the raw data from the byte array", e);
        }
    }

    public OperationResponse copyObject(OperationResponse operation) {
        return operation.copyObject();
    }

    private OperationResponse copyObject() {
        return OperationResponse.newBuilder()
                .setOperationType(operationType)
                .setUpdateOperationResponse(updateOperationResponse)
                .setInsertOperationResponse(insertOperationResponse)
                .setDeleteOperationResponse(deleteOperationResponse)
                .setUpsertOperationResponse(upsertOperationResponse)
                .setScanQueryOperationResponse(scanQueryOperationResponse)
                .build();
    }

    @Override
    public String toString() {
        return "OperationResponse{" +
                "operationType=" + operationType +
                ", message='" + message + '\'' +
                ", deleteOperationResponse=" + deleteOperationResponse +
                ", insertOperationResponse=" + insertOperationResponse +
                ", scanQueryOperationResponse=" + scanQueryOperationResponse +
                ", updateOperationResponse=" + updateOperationResponse +
                ", upsertOperationResponse=" + upsertOperationResponse +
                '}';
    }
}
