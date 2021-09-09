package org.apache.hadoop.hetu.photon.operation.response;

import org.apache.hadoop.hetu.photon.ClientTestUtil;
import org.apache.hadoop.hetu.photon.operation.OperationType;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 * Created by xiliu on 2021/9/3
 */
public class TestScanQueryOperationResponse {
    @Test
    public void scanQueryOperationResponse() {
        OperationResponse operationResponse = OperationResponse.newBuilder()
                .setOperationType(OperationType.SCAN_QUERY)
                .setScanQueryOperationResponse(new ScanQueryOperationResponse(Arrays.asList(ClientTestUtil.getPartialRowWithAllTypes())))
                .setMessage("Scan query operation exec successful.")
                .build();

        Assert.assertTrue(operationResponse.getOperationType().equals(OperationType.SCAN_QUERY));
        Assert.assertNotNull(operationResponse.getMessage());
        Assert.assertTrue(operationResponse.getScanQueryOperationResponse().getRowList().size() > 0);
    }
}
