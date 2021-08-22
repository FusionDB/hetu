package org.apache.hadoop.hetu.photon.helpers;

/**
 * Created by xiliu on 2021/8/22
 */
public class InsertOperation extends Operation {
    private final static OperationType operationType = OperationType.INSERT;

    public InsertOperation(PartialRow row) {
        super(row, operationType);
    }

    public InsertOperation(PartialRow row, long rowOperationSizeBytes, String conditionalExpression) {
        super(row, operationType, rowOperationSizeBytes, conditionalExpression);
    }

    public InsertOperation(PartialRow row, long rowOperationSizeBytes) {
        super(row, operationType, rowOperationSizeBytes);
    }
}
