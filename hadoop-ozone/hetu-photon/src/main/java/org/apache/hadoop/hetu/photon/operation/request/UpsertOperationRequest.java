package org.apache.hadoop.hetu.photon.operation.request;

import org.apache.hadoop.hetu.photon.proto.HetuPhotonProtos.UpsertOperationRequestProto;

/**
 * Created by xiliu on 2021/9/3
 */
public class UpsertOperationRequest {
    private String conditionalExpression;

    public UpsertOperationRequest(String conditionalExpression) {
        this.conditionalExpression = conditionalExpression;
    }

    public String getConditionalExpression() {
        return conditionalExpression;
    }

    public UpsertOperationRequestProto toProto() {
        UpsertOperationRequestProto.Builder operationRequest = UpsertOperationRequestProto
                .newBuilder();

        if (conditionalExpression != null) {
            operationRequest.setConditionalExpression(conditionalExpression);
        }
        return operationRequest.build();
    }

    public static UpsertOperationRequest fromProtobuf(UpsertOperationRequestProto operationRequest) {
        return new UpsertOperationRequest(operationRequest.getConditionalExpression());
    }
}
