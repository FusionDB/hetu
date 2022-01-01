// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.hadoop.hetu.photon.helpers;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import org.apache.hadoop.hetu.photon.meta.common.ColumnType;
import org.apache.hadoop.hetu.photon.meta.schema.ColumnSchema;
import org.apache.hadoop.hetu.photon.meta.schema.Schema;
import org.apache.hadoop.hetu.photon.proto.HetuPhotonProtos.RowOperationsPB;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

/**
 *
 * TODO(todd): this should not extend Rpc. Rather, we should make single-operation writes
 * just use a Batch instance with a single rowOperations in it.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class Operation {
  private static final Logger LOG = LoggerFactory.getLogger(Operation.class);

  /**
   * This size will be set when serialize is called. It stands for the size of the row in this
   * operation.
   */
  private long rowOperationSizeBytes = 0;

  private long txnId = -1;

  enum ChangeType {
    INSERT((byte) RowOperationsPB.Type.INSERT.getNumber()),
    UPDATE((byte)RowOperationsPB.Type.UPDATE.getNumber()),
    DELETE((byte)RowOperationsPB.Type.DELETE.getNumber()),
    SPLIT_ROWS((byte)RowOperationsPB.Type.SPLIT_ROW.getNumber()),
    UPSERT((byte)RowOperationsPB.Type.UPSERT.getNumber()),
    RANGE_LOWER_BOUND((byte) RowOperationsPB.Type.RANGE_LOWER_BOUND.getNumber()),
    RANGE_UPPER_BOUND((byte) RowOperationsPB.Type.RANGE_UPPER_BOUND.getNumber()),
    EXCLUSIVE_RANGE_LOWER_BOUND(
        (byte) RowOperationsPB.Type.EXCLUSIVE_RANGE_LOWER_BOUND.getNumber()),
    INCLUSIVE_RANGE_UPPER_BOUND(
        (byte) RowOperationsPB.Type.INCLUSIVE_RANGE_UPPER_BOUND.getNumber()),
    INSERT_IGNORE((byte) RowOperationsPB.Type.INSERT_IGNORE.getNumber()),
    UPDATE_IGNORE((byte) RowOperationsPB.Type.UPDATE_IGNORE.getNumber()),
    DELETE_IGNORE((byte) RowOperationsPB.Type.DELETE_IGNORE.getNumber());

    ChangeType(byte encodedByte) {
      this.encodedByte = encodedByte;
    }

    byte toEncodedByte() {
      return encodedByte;
    }

    /** The byte used to encode this in a RowOperationsPB */
    private final byte encodedByte;
  }

  private PartialRow row;

  public Operation(Schema schema) {
      this.row = schema.newPartialRow();
  }

  /**
   * Classes extending Operation need to have a specific ChangeType
   * @return Operation's ChangeType
   */
  abstract ChangeType getChangeType();

  /**
   * Returns the size in bytes of this operation's row after serialization.
   * @return size in bytes
   * @throws IllegalStateException thrown if this RPC hasn't been serialized eg sent to a TS
   */
  long getRowOperationSizeBytes() {
    if (this.rowOperationSizeBytes == 0) {
      throw new IllegalStateException("This row hasn't been serialized yet");
    }
    return this.rowOperationSizeBytes;
  }

  /**
   * Get the underlying row to modify.
   * @return a partial row that will be sent with this Operation
   */
  public PartialRow getRow() {
    return this.row;
  }

  /**
   * Helper method that puts a list of Operations together into a WriteRequestPB.
   * @param operations The list of ops to put together in a WriteRequestPB
   * @return A fully constructed WriteRequestPB containing the passed rows, or
   *         null if no rows were passed.
   */
  public static RowOperationsPB builderRowOperationsPB(List<Operation> operations) {
    if (operations == null || operations.isEmpty()) {
      return null;
    }
    RowOperationsPB rowOps = new OperationsEncoder().encodeOperations(operations);
    if (rowOps == null) {
      return null;
    }
    return rowOps;
  }

  static class OperationsEncoder {
    private Schema schema;
    private ByteBuffer rows;
    // We're filling this list as we go through the operations in encodeRow() and at the same time
    // compute the total size, which will be used to right-size the array in toPB().
    private List<ByteBuffer> indirect;
    private long indirectWrittenBytes;

    /**
     * Initializes the state of the encoder based on the schema and number of operations to encode.
     *
     * @param schema the schema of the table which the operations belong to.
     * @param numOperations the number of operations.
     */
    private void init(Schema schema, int numOperations) {
      this.schema = schema;

      // Set up the encoded data.
      // Estimate a maximum size for the data. This is conservative, but avoids
      // having to loop through all the operations twice.
      final int columnBitSetSize = Bytes.getBitSetSize(schema.getColumnCount());
      int sizePerRow = 1 /* for the op type */ + schema.getRowSize() + columnBitSetSize;
      if (schema.hasNullableColumns()) {
        // nullsBitSet is the same size as the columnBitSet
        sizePerRow += columnBitSetSize;
      }

      // TODO: would be more efficient to use a buffer which "chains" smaller allocations
      // instead of a doubling buffer like BAOS.
      this.rows = ByteBuffer.allocate(sizePerRow * numOperations)
                            .order(ByteOrder.LITTLE_ENDIAN);
      this.indirect = new ArrayList<>(schema.getVarLengthColumnCount() * numOperations);
    }

    /**
     * Builds the row operations protobuf message with encoded operations.
     * @return the row operations protobuf message.
     */
    private RowOperationsPB toPB() {
      RowOperationsPB.Builder rowOpsBuilder = RowOperationsPB.newBuilder();

      // TODO: we could avoid a copy here by using an implementation that allows
      // zero-copy on a slice of an array.
      rows.limit(rows.position());
      rows.flip();
      rowOpsBuilder.setSchema(this.schema.toProtobuf());
      rowOpsBuilder.setRows(ByteString.copyFrom(rows));
      if (indirect.size() > 0) {
        // TODO: same as above, we could avoid a copy here by using an implementation that allows
        // zero-copy on a slice of an array.
        byte[] indirectData = new byte[(int)indirectWrittenBytes];
        int offset = 0;
        for (ByteBuffer bb : indirect) {
          int bbSize = bb.remaining();
          bb.get(indirectData, offset, bbSize);
          offset += bbSize;
        }
        rowOpsBuilder.setIndirectData(UnsafeByteOperations.unsafeWrap(indirectData));
      }
      return rowOpsBuilder.build();
    }

    private void encodeRow(PartialRow row, ChangeType type) {
      int columnCount = row.getSchema().getColumnCount();
      BitSet columnsBitSet = row.getColumnsBitSet();
      BitSet nullsBitSet = row.getNullsBitSet();

      // If this is a DELETE operation only the key columns should to be set.
      if (type == ChangeType.DELETE || type == ChangeType.DELETE_IGNORE) {
        columnCount = row.getSchema().getPrimaryKeyColumnCount();
        // Clear the bits indicating any non-key fields are set.
        columnsBitSet.clear(schema.getPrimaryKeyColumnCount(), columnsBitSet.size());
        if (schema.hasNullableColumns()) {
          nullsBitSet.clear(schema.getPrimaryKeyColumnCount(), nullsBitSet.size());
        }
      }

      rows.put(type.toEncodedByte());
      System.out.println("0: type: " + type.name());
      rows.put(Bytes.fromBitSet(columnsBitSet, schema.getColumnCount()));
      System.out.println("1: columnsBitSet: " + columnsBitSet);
      if (schema.hasNullableColumns()) {
        rows.put(Bytes.fromBitSet(nullsBitSet, schema.getColumnCount()));
        System.out.println("2: nullsBitSet: " + nullsBitSet);
      }

      byte[] rowData = row.getRowAlloc();
      int currentRowOffset = 0;
      for (int colIdx = 0; colIdx < columnCount; colIdx++) {
        ColumnSchema col = schema.getColumnByIndex(colIdx);
        // Keys should always be specified, maybe check?
        if (row.isSet(colIdx) && !row.isSetToNull(colIdx)) {
          if (col.getColumnType() == ColumnType.STRING || col.getColumnType() == ColumnType.BINARY ||
              col.getColumnType() == ColumnType.VARCHAR) {
            ByteBuffer varLengthData = row.getVarLengthData().get(colIdx);
            varLengthData.reset();
            rows.putLong(indirectWrittenBytes);
            int bbSize = varLengthData.remaining();
            rows.putLong(bbSize);
            System.out.println("| varlen bbSize: " + bbSize);
            indirect.add(varLengthData);
            indirectWrittenBytes += bbSize;
            System.out.println("| varlen indirectWrittenBytes: " + indirectWrittenBytes);
          } else {
            // This is for cols other than strings
            rows.put(rowData, currentRowOffset, col.getTypeSize());
            LOG.error("<---- rowData: {}, currentRowOffset: {}, colSize: {} ----->", Arrays.toString(rowData), currentRowOffset, col.getTypeSize());
          }
        }
        currentRowOffset += col.getTypeSize();
      }
    }

    public RowOperationsPB encodeOperations(List<Operation> operations) {
      if (operations == null || operations.isEmpty()) {
        return null;
      }
      init(operations.get(0).row.getSchema(), operations.size());
      for (Operation operation : operations) {
        encodeRow(operation.row, operation.getChangeType());
      }
      return toPB();
    }

    public RowOperationsPB encodeRangePartitions() {
      throw new UnsupportedOperationException("encodeRangePartitions");
    }

    public RowOperationsPB encodeLowerAndUpperBounds(PartialRow lowerBound,
                                                     PartialRow upperBound) {
      throw new UnsupportedOperationException("encodeLowerAndUpperBounds");
    }
  }
}
