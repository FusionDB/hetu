package org.apache.hadoop.hetu.photon.operation.response;

import org.apache.hadoop.hetu.photon.proto.HetuPhotonProtos.InsertOperationResponseProto;

/**
 * Created by xiliu on 2021/9/3
 */
public class InsertOperationResponse {
    public static InsertOperationResponse fromProtobuf(InsertOperationResponseProto insertOperation) {
        return null;
    }

    public InsertOperationResponseProto toProto() {
       return InsertOperationResponseProto.newBuilder().build();
    }
}
