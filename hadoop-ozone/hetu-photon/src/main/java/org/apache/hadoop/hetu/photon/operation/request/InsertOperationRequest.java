package org.apache.hadoop.hetu.photon.operation.request;

import org.apache.hadoop.hetu.photon.helpers.PartialRow;
import org.apache.hadoop.hetu.photon.proto.HetuPhotonProtos.InsertOperationRequestProto;

/**
 * Created by xiliu on 2021/9/3
 */
public class InsertOperationRequest {
    private PartialRow row;
    private long rowSizeBytes = 0;

    public InsertOperationRequest(PartialRow row, long rowSizeBytes) {
        this.row = row;
        this.rowSizeBytes = rowSizeBytes;
    }

    public InsertOperationRequest(PartialRow row) {
        this.row = row;
    }

    public PartialRow getRow() {
        return row;
    }

    public long getRowSizeBytes() {
        if (this.rowSizeBytes == 0) {
            throw new IllegalStateException("This row hasn't been serialized yet");
        }
        return this.rowSizeBytes;
    }

    public InsertOperationRequestProto toProto() {
        InsertOperationRequestProto.Builder operationRequest = InsertOperationRequestProto.newBuilder()
                .setRowSizeBytes(rowSizeBytes)
                .setRow(row.toProtobuf());
        return operationRequest.build();
    }

    public static InsertOperationRequest fromProtobuf(InsertOperationRequestProto operationRequest) {
        return new InsertOperationRequest(PartialRow.fromProtobuf(operationRequest.getRow()),
                operationRequest.getRowSizeBytes());
    }

    @Override
    public String toString() {
        return "InsertOperationRequest{" +
                "row=" + row +
                ", rowSizeBytes=" + rowSizeBytes +
                '}';
    }

    public InsertOperationRequest copyObject() {
        return new InsertOperationRequest(row, rowSizeBytes);
    }
}
