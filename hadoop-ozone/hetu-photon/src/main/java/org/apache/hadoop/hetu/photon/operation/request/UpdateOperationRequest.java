package org.apache.hadoop.hetu.photon.operation.request;

import org.apache.hadoop.hetu.photon.proto.HetuPhotonProtos.UpdateOperationRequestProto;

/**
 * Created by xiliu on 2021/9/3
 */
public class UpdateOperationRequest {
    private String conditionalExpression;

    public UpdateOperationRequest(String conditionalExpression) {
        this.conditionalExpression = conditionalExpression;
    }

    public String getConditionalExpression() {
        return conditionalExpression;
    }

    public UpdateOperationRequestProto toProto() {
        UpdateOperationRequestProto.Builder operationRequest = UpdateOperationRequestProto
                .newBuilder();

        if (conditionalExpression != null) {
            operationRequest.setConditionalExpression(conditionalExpression);
        }
        return operationRequest.build();
    }

    public static UpdateOperationRequest fromProtobuf(UpdateOperationRequestProto operationRequest) {
        return new UpdateOperationRequest(operationRequest.getConditionalExpression());
    }
}
