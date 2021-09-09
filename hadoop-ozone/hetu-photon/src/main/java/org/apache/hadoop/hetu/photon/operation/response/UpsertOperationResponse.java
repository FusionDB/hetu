package org.apache.hadoop.hetu.photon.operation.response;

import org.apache.hadoop.hetu.photon.proto.HetuPhotonProtos.UpsertOperationResponseProto;

/**
 * Created by xiliu on 2021/9/3
 */
public class UpsertOperationResponse {
    public static UpsertOperationResponse fromProtobuf(UpsertOperationResponseProto upsertOperation) {
        return null;
    }

    public UpsertOperationResponseProto toProto() {
        return UpsertOperationResponseProto.newBuilder().build();
    }
}
