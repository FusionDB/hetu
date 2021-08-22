package org.apache.hadoop.hetu.photon.encoder;

import org.apache.hadoop.hetu.photon.helpers.InsertOperation;
import org.apache.hadoop.hetu.photon.helpers.Operation;

import java.io.IOException;

public class InsertOperationEncoder extends OperationEncoder {
    @Override
    public byte[] toPersistedFormat(Operation object) throws IOException {
        return super.toPersistedFormat(object);
    }

    public InsertOperation fromPersistedFormat(byte[] rawData) throws IOException {
        Operation operation = super.fromPersistedFormat(rawData);
        return new InsertOperation(operation.getRow(),
                operation.getRowOperationSizeBytes(),
                operation.getConditionalExpression());
    }

    @Override
    public Operation copyObject(Operation operation) {
        return new InsertOperation(operation.getRow(),
                operation.getRowOperationSizeBytes(),
                operation.getConditionalExpression());
    }
}
