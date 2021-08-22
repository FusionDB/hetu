package org.apache.hadoop.hetu.photon;

import org.apache.hadoop.hetu.photon.helpers.Operation;
import org.apache.hadoop.hetu.photon.helpers.OperationType;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by xiliu on 2021/8/20
 */
public class TestOperation {
    @Test
    public void OperationInsert() {
        Operation operation = new Operation(ClientTestUtil.getPartialRowWithAllTypes(), OperationType.INSERT);
        Assert.assertTrue(operation.getOperationType().equals(OperationType.INSERT));
    }
}
