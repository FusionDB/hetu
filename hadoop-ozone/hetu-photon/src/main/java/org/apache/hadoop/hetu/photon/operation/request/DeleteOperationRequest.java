package org.apache.hadoop.hetu.photon.operation.request;

import org.apache.hadoop.hetu.photon.proto.HetuPhotonProtos.DeleteOperationRequestProto;

/**
 * Created by xiliu on 2021/9/3
 */
public class DeleteOperationRequest {
    private String conditionalExpression;

    public DeleteOperationRequest(String conditionalExpression) {
        this.conditionalExpression = conditionalExpression;
    }

    public String getConditionalExpression() {
        return conditionalExpression;
    }

    public DeleteOperationRequestProto toProto() {
        DeleteOperationRequestProto.Builder operationRequest = DeleteOperationRequestProto
                .newBuilder();

        if (conditionalExpression != null) {
            operationRequest.setConditionalExpression(conditionalExpression);
        }
        return operationRequest.build();
    }

    public static DeleteOperationRequest fromProtobuf(DeleteOperationRequestProto operationRequest) {
        return new DeleteOperationRequest(operationRequest.getConditionalExpression());
    }
}
