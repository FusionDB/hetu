package org.apache.hadoop.hetu.photon.operation.response;

import org.apache.hadoop.hetu.photon.helpers.PartialRow;
import org.apache.hadoop.hetu.photon.proto.HetuPhotonProtos.ScanQueryOperationResponseProto;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by xiliu on 2021/9/3
 */
public class ScanQueryOperationResponse {
    private long rowSizeBytes;
    private List<PartialRow> rowList;

    public ScanQueryOperationResponse(long rowSizeBytes, List<PartialRow> rowList) {
        this.rowSizeBytes = rowSizeBytes;
        this.rowList = rowList;
    }

    public ScanQueryOperationResponse(List<PartialRow> rowList) {
        this.rowList = rowList;
    }

    public long getRowSizeBytes() {
        return rowSizeBytes;
    }

    public List<PartialRow> getRowList() {
        return rowList;
    }

    public ScanQueryOperationResponseProto toProto() {
        ScanQueryOperationResponseProto.Builder operationResponse = ScanQueryOperationResponseProto
                .newBuilder().setRowSizeBytes(rowSizeBytes);
        if (rowList.size() > 0) {
            operationResponse.addAllRow(rowList.stream()
                    .map(row -> row.toProtobuf())
                    .collect(Collectors.toList())
            );
        }
        return operationResponse.build();
    }

    public static ScanQueryOperationResponse fromProtobuf(ScanQueryOperationResponseProto operationResponse) {
        return new ScanQueryOperationResponse(operationResponse.getRowSizeBytes(),
                operationResponse.getRowList()
                        .stream()
                        .map(row -> PartialRow.fromProtobuf(row))
                        .collect(Collectors.toList())
        );
    }

    @Override
    public String toString() {
        return "ScanQueryOperationResponse{" +
                "rowSizeBytes=" + rowSizeBytes +
                ", rowList=" + rowList +
                '}';
    }

    public ScanQueryOperationResponse copyObject() {
        return new ScanQueryOperationResponse(rowSizeBytes, rowList);
    }
}
