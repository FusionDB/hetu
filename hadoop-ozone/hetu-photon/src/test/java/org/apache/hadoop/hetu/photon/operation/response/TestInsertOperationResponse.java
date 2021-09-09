package org.apache.hadoop.hetu.photon.operation.response;

import org.apache.hadoop.hetu.photon.operation.OperationType;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by xiliu on 2021/9/3
 */
public class TestInsertOperationResponse {

    @Test
    public void InsertOperationResponse() {
        OperationResponse operationResponse = OperationResponse.newBuilder()
                .setOperationType(OperationType.INSERT)
                .setMessage("insert into fail")
                .setInsertOperationResponse(new InsertOperationResponse())
                .build();
        Assert.assertTrue(operationResponse.getOperationType().equals(OperationType.INSERT));
        Assert.assertNotNull(operationResponse.getMessage());
    }
}
