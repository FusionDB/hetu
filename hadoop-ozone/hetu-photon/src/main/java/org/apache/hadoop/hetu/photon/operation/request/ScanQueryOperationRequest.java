package org.apache.hadoop.hetu.photon.operation.request;

import org.apache.hadoop.hetu.photon.proto.HetuPhotonProtos.ScanQueryOperationRequestProto;

/**
 * Created by xiliu on 2021/9/3
 */
public class ScanQueryOperationRequest {
    private String conditionalExpression;

    public ScanQueryOperationRequest(String conditionalExpression) {
        this.conditionalExpression = conditionalExpression;
    }

    public String getConditionalExpression() {
        return conditionalExpression;
    }

    public ScanQueryOperationRequestProto toProto() {
        ScanQueryOperationRequestProto.Builder operationRequest = ScanQueryOperationRequestProto
                .newBuilder();

        if (conditionalExpression != null) {
            operationRequest.setConditionalExpression(conditionalExpression);
        }
        return operationRequest.build();
    }

    public static ScanQueryOperationRequest fromProtobuf(ScanQueryOperationRequestProto operationRequest) {
        return new ScanQueryOperationRequest(operationRequest.getConditionalExpression());
    }
}
