package org.apache.hadoop.ozone.om.codec;

import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.ozone.om.helpers.OmPartitionInfo;
import org.apache.hadoop.ozone.om.helpers.OmTableInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;

import java.io.IOException;

/**
 * Created by xiliu on 2021/6/24
 */
public class OmPartitionInfoCodec implements Codec<OmPartitionInfo> {
    @Override
    public byte[] toPersistedFormat(OmPartitionInfo object) throws IOException {
        Preconditions
                .checkNotNull(object, "Null object can't be converted to byte array.");
        return object.getProtobuf().toByteArray();
    }

    @Override
    public OmPartitionInfo fromPersistedFormat(byte[] rawData) throws IOException {
        Preconditions
                .checkNotNull(rawData,
                        "Null byte array can't converted to real object.");
        try {
            return OmPartitionInfo.getFromProtobuf(OzoneManagerProtocolProtos.PartitionInfo.parseFrom(rawData));
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException(
                    "Can't encode the the raw data from the byte array", e);
        }
    }

    @Override
    public OmPartitionInfo copyObject(OmPartitionInfo object) {
        return object.copyObject();
    }
}
