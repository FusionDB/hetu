package org.apache.hadoop.hetu.photon.operation.response;

import org.apache.hadoop.hetu.photon.proto.HetuPhotonProtos.UpdateOperationResponseProto;

/**
 * Created by xiliu on 2021/9/3
 */
public class UpdateOperationResponse {

    public static UpdateOperationResponse fromProtobuf(UpdateOperationResponseProto updateOperation) {
        return null;
    }

    public UpdateOperationResponseProto toProto() {
        return UpdateOperationResponseProto.newBuilder().build();
    }
}
