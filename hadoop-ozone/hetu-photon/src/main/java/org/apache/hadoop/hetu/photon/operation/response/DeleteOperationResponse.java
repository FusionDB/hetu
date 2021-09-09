package org.apache.hadoop.hetu.photon.operation.response;

import org.apache.hadoop.hetu.photon.proto.HetuPhotonProtos.DeleteOperationResponseProto;

/**
 * Created by xiliu on 2021/9/3
 */
public class DeleteOperationResponse {
    public static DeleteOperationResponse fromProtobuf(DeleteOperationResponseProto deleteOperation) {
        return null;
    }

    public DeleteOperationResponseProto toProto() {
        return DeleteOperationResponseProto.newBuilder().build();
    }
}
