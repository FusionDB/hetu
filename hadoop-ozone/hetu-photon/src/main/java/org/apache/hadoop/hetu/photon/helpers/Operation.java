package org.apache.hadoop.hetu.photon.helpers;

import org.apache.hadoop.hetu.photon.proto.HetuPhotonProtos.OperationProto;

/**
 * Created by xiliu on 2021/8/16
 */
public class Operation {
    private PartialRow row;
    private OperationType operationType;
    private long rowOperationSizeBytes = 0;
    private String conditionalExpression;

    public Operation(PartialRow row, OperationType operationType,
                     long rowOperationSizeBytes, String conditionalExpression) {
        this.row = row;
        this.operationType = operationType;
        this.rowOperationSizeBytes = rowOperationSizeBytes;
        this.conditionalExpression = conditionalExpression;
    }

    public Operation(PartialRow row, OperationType operationType, long rowOperationSizeBytes) {
        this.row = row;
        this.operationType = operationType;
        this.rowOperationSizeBytes = rowOperationSizeBytes;
    }

    public Operation(PartialRow row, OperationType operationType) {
        this.row = row;
        this.operationType = operationType;
    }

    public PartialRow getRow() {
        return row;
    }

    public OperationType getOperationType() {
        return operationType;
    }

    public long getRowOperationSizeBytes() {
        if (this.rowOperationSizeBytes == 0) {
            throw new IllegalStateException("This row hasn't been serialized yet");
        }
        return this.rowOperationSizeBytes;
    }

    public String getConditionalExpression() {
        return conditionalExpression;
    }

    public OperationProto toProto() {
        OperationProto.Builder operationProto = OperationProto.newBuilder()
                .setOperationType(operationType.toProto())
                .setRowOperationSizeBytes(rowOperationSizeBytes)
                .setRow(row.toProtobuf());
        if (conditionalExpression != null) {
            operationProto.setConditionalExpression(conditionalExpression);
        }
        return operationProto.build();
    }

    public static Operation fromProtobuf(OperationProto operation) {
        return new Operation(PartialRow.fromProtobuf(operation.getRow()),
                OperationType.valueOf(operation.getOperationType()),
                operation.getRowOperationSizeBytes(),
                operation.getConditionalExpression());
    }

    @Override
    public String toString() {
        return "Operation{" +
                "row=" + row +
                ", operationType=" + operationType +
                ", rowOperationSizeBytes=" + rowOperationSizeBytes +
                ", conditionalExpression='" + conditionalExpression + '\'' +
                '}';
    }

    public Operation copyObject() {
        return new Operation(row, operationType,
                rowOperationSizeBytes, conditionalExpression);
    }
}
