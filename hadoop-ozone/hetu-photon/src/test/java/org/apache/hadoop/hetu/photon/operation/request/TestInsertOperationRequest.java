package org.apache.hadoop.hetu.photon.operation.request;

import org.apache.hadoop.hetu.photon.ClientTestUtil;
import org.apache.hadoop.hetu.photon.operation.OperationType;
import org.apache.hadoop.hetu.photon.operation.request.InsertOperationRequest;
import org.apache.hadoop.hetu.photon.operation.request.OperationRequest;
import org.apache.hadoop.hetu.photon.operation.response.InsertOperationResponse;
import org.apache.hadoop.hetu.photon.operation.response.OperationResponse;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by xiliu on 2021/8/20
 */
public class TestInsertOperationRequest {
    @Test
    public void InsertOperationRequest() {
        OperationRequest operationRequest = OperationRequest.newBuilder()
                .setOperationType(OperationType.INSERT)
                .setInsertOperationRequest(new InsertOperationRequest(ClientTestUtil.getPartialRowWithAllTypes()))
                .build();

        Assert.assertTrue(operationRequest.getOperationType().equals(OperationType.INSERT));
        Assert.assertTrue(operationRequest.getInsertOperationRequest().getRow().getVarLengthData().size() > 0);
    }

}
