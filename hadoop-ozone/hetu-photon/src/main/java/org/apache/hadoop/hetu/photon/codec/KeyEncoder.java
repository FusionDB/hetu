package org.apache.hadoop.hetu.photon.codec;

import org.apache.hadoop.hetu.photon.meta.common.ColumnType;
import org.apache.hadoop.hetu.photon.meta.table.ColumnSchema;
import org.apache.hadoop.hetu.photon.meta.table.Schema;
import org.apache.hadoop.hetu.photon.meta.util.ByteVec;
import org.apache.hadoop.hetu.photon.helpers.Bytes;
import org.apache.hadoop.hetu.photon.helpers.PartialRow;

import java.nio.ByteBuffer;

/**
 * Created by xiliu on 2021/8/18
 */
public class KeyEncoder {
    /**
     * Encodes the primary key of the row.
     *
     * @param row the row to encode
     * @return the encoded primary key of the row
     */
    public static byte[] encodePrimaryKey(final PartialRow row) {
        ByteVec buf = ByteVec.create();
        final Schema schema = row.getSchema();
        for (int columnIdx = 0; columnIdx < schema.getPrimaryKeyColumnCount(); columnIdx++) {
            final boolean isLast = columnIdx + 1 == schema.getPrimaryKeyColumnCount();
            encodeColumn(row, columnIdx, isLast, buf);
        }
        return buf.toArray();
    }

    private static void encodeColumn(PartialRow row,
                                     int columnIdx,
                                     boolean isLast,
                                     ByteVec buf) {
        final Schema schema = row.getSchema();
        final ColumnSchema column = schema.getColumnByIndex(columnIdx);
        if (!row.isSet(columnIdx)) {
            throw new IllegalStateException(String.format("Primary key column %s is not set",
                    column.getColumnName()));
        }
        final ColumnType type = column.getColumnType();
        if (type == ColumnType.STRING || type == ColumnType.BINARY ||
                type == ColumnType.VARCHAR) {
            encodeBinary(row.getVarLengthData().get(columnIdx), isLast, buf);
        } else {
            encodeSignedInt(row.getRowAlloc(),
                    schema.getColumnOffset(columnIdx),
                    column.getTypeSize(),
                    buf);
        }
        System.out.println(column.getColumnName() + ": " + column.getTypeSize());
    }

    /**
     * Encodes a variable length binary value into the output buffer.
     * @param value the value to encode
     * @param isLast whether the value is the final component in the key
     * @param buf the output buffer
     */
    private static void encodeBinary(ByteBuffer value, boolean isLast, ByteVec buf) {
        value.reset();

        // TODO find a way to not have to read byte-by-byte that doesn't require extra copies. This is
        // especially slow now that users can pass direct byte buffers.
        while (value.hasRemaining()) {
            byte currentByte = value.get();
            buf.push(currentByte);
            if (!isLast && currentByte == 0x00) {
                // If we're a middle component of a composite key, we need to add a \x00
                // at the end in order to separate this component from the next one. However,
                // if we just did that, we'd have issues where a key that actually has
                // \x00 in it would compare wrong, so we have to instead add \x00\x00, and
                // encode \x00 as \x00\x01. -- key_encoder.h
                buf.push((byte) 0x01);
            }
        }

        if (!isLast) {
            buf.push((byte) 0x00);
            buf.push((byte) 0x00);
        }
    }

    /**
     * Encodes a signed integer into the output buffer
     *
     * @param value an array containing the little-endian encoded integer
     * @param offset the offset of the value into the value array
     * @param len the width of the value
     * @param buf the output buffer
     */
    private static void encodeSignedInt(byte[] value,
                                        int offset,
                                        int len,
                                        ByteVec buf) {
        // Picking the first byte because big endian.
        byte lastByte = value[offset + (len - 1)];
        lastByte = Bytes.xorLeftMostBit(lastByte);
        buf.push(lastByte);
        for (int i = len - 2; i >= 0; i--) {
            buf.push(value[offset + i]);
        }
    }
}
