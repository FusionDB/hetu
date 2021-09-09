package org.apache.hadoop.hetu.photon.operation.request;

import org.apache.hadoop.hetu.photon.operation.OperationType;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by xiliu on 2021/9/3
 */
public class TestScanQueryOperationRequest {
    @Test
    public void scanQueryOperationRequest() {
        OperationRequest operationRequest = OperationRequest.newBuilder()
                .setOperationType(OperationType.SCAN_QUERY)
                .setScanQueryOperationRequest(new ScanQueryOperationRequest("id = 100 and aget > 30"))
                .build();

        Assert.assertTrue(operationRequest.getOperationType().equals(OperationType.SCAN_QUERY));
        Assert.assertNotNull(operationRequest.getScanQueryOperationRequest().getConditionalExpression());
    }
}
