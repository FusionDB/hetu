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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Timestamp;

import org.apache.hadoop.hetu.photon.meta.common.ColumnType;
import org.apache.hadoop.hetu.photon.meta.common.ColumnTypeAttributes;
import org.apache.hadoop.hetu.photon.meta.table.ColumnSchema;
import org.apache.hadoop.hetu.photon.meta.table.Schema;
import org.apache.hadoop.hetu.photon.meta.util.TimestampUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * RowResult represents one row from a scanner, in columnar layout.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
class ColumnarRowResult extends RowResult {

    private final Slice[] data;
    private final Slice[] varlenData;
    private final Slice[] nonNullBitmaps;

    /**
     * Prepares the row representation using the provided data. Doesn't copy data
     * out of the byte arrays. Package private.
     * @param schema Schema used to build the rowData
     * @param data The raw columnar data corresponding to the primitive-typed columns
     * @param varlenData The variable-length data for the variable-length-typed columns
     * @param nonNullBitmaps The bitmaps corresponding to the non-null status of the cells
     * @param rowIndex The index of the row in data/varlenData/nonNullBitmaps
     */
    ColumnarRowResult(Schema schema, Slice[] data, Slice[] varlenData, Slice[] nonNullBitmaps,
                      int rowIndex) {
        super(schema, rowIndex);
        this.data = data;
        this.varlenData = varlenData;
        this.nonNullBitmaps = nonNullBitmaps;
        advancePointerTo(rowIndex);
    }

    /**
     * Get the specified column's integer
     * @param columnIndex Column index in the schema
     * @return an integer
     * @throws IllegalArgumentException if the column is null
     * or if the type doesn't match the column's type
     * @throws IndexOutOfBoundsException if the column doesn't exist
     */
    @Override
    public int getInt(int columnIndex) {
        checkValidColumn(columnIndex);
        checkNull(columnIndex);
        checkType(columnIndex, ColumnType.INT32, ColumnType.DATE);
        return Bytes.getInt(this.data[columnIndex].getRawArray(),
                this.data[columnIndex].getRawOffset() + index * 4);
    }

    /**
     * Get the specified column's short
     * @param columnIndex Column index in the schema
     * @return a short
     * @throws IllegalArgumentException if the column is null
     * or if the type doesn't match the column's type
     * @throws IndexOutOfBoundsException if the column doesn't exist
     */
    @Override
    public short getShort(int columnIndex) {
        checkValidColumn(columnIndex);
        checkNull(columnIndex);
        checkType(columnIndex, ColumnType.INT16);
        return Bytes.getShort(this.data[columnIndex].getRawArray(),
                this.data[columnIndex].getRawOffset() + index * 2);
    }

    /**
     * Get the specified column's boolean
     * @param columnIndex Column index in the schema
     * @return a boolean
     * @throws IllegalArgumentException if the column is null
     * or if the type doesn't match the column's type
     * @throws IndexOutOfBoundsException if the column doesn't exist
     */
    @Override
    public boolean getBoolean(int columnIndex) {
        checkValidColumn(columnIndex);
        checkNull(columnIndex);
        checkType(columnIndex, ColumnType.BOOL);
        byte b = Bytes.getByte(this.data[columnIndex].getRawArray(),
                this.data[columnIndex].getRawOffset() + index);
        return b == 1;
    }

    /**
     * Get the specified column's byte
     * @param columnIndex Column index in the schema
     * @return a byte
     * @throws IllegalArgumentException if the column is null
     * or if the type doesn't match the column's type
     * @throws IndexOutOfBoundsException if the column doesn't exist
     */
    @Override
    public byte getByte(int columnIndex) {
        checkValidColumn(columnIndex);
        checkNull(columnIndex);
        checkType(columnIndex, ColumnType.INT8);
        return Bytes.getByte(this.data[columnIndex].getRawArray(),
                this.data[columnIndex].getRawOffset() + index);
    }

    /**
     * Get the specified column's float
     * @param columnIndex Column index in the schema
     * @return a float
     * @throws IllegalArgumentException if the column is null
     * or if the type doesn't match the column's type
     * @throws IndexOutOfBoundsException if the column doesn't exist
     */
    @Override
    public float getFloat(int columnIndex) {
        checkValidColumn(columnIndex);
        checkNull(columnIndex);
        checkType(columnIndex, ColumnType.FLOAT);
        return Bytes.getFloat(this.data[columnIndex].getRawArray(),
                this.data[columnIndex].getRawOffset() + index * 4);
    }

    /**
     * Get the specified column's double
     * @param columnIndex Column index in the schema
     * @return a double
     * @throws IllegalArgumentException if the column is null
     * or if the type doesn't match the column's type
     * @throws IndexOutOfBoundsException if the column doesn't exist
     */
    @Override
    public double getDouble(int columnIndex) {
        checkValidColumn(columnIndex);
        checkNull(columnIndex);
        checkType(columnIndex, ColumnType.DOUBLE);
        return Bytes.getDouble(this.data[columnIndex].getRawArray(),
                this.data[columnIndex].getRawOffset() + index * 8);
    }

    /**
     * Get the specified column's Decimal.
     *
     * @param columnIndex Column index in the schema
     * @return a BigDecimal.
     * @throws IllegalArgumentException if the column is null
     * @throws IndexOutOfBoundsException if the column doesn't exist
     */
    @Override
    public BigDecimal getDecimal(int columnIndex) {
        checkValidColumn(columnIndex);
        checkNull(columnIndex);
        checkType(columnIndex, ColumnType.DECIMAL);
        ColumnSchema column = schema.getColumnByIndex(columnIndex);
        ColumnTypeAttributes typeAttributes = column.getTypeAttributes();
        return Bytes.getDecimal(this.data[columnIndex].getRawArray(),
                this.data[columnIndex].getRawOffset() + ColumnType.DECIMAL.getSize(typeAttributes) * index,
                typeAttributes.getPrecision(), typeAttributes.getScale());
    }

    /**
     * Get the specified column's Timestamp.
     *
     * @param columnIndex Column index in the schema
     * @return a Timestamp
     * @throws IllegalArgumentException if the column is null, is unset,
     * or if the type doesn't match the column's type
     * @throws IndexOutOfBoundsException if the column doesn't exist
     */
    @Override
    public Timestamp getTimestamp(int columnIndex) {
        checkValidColumn(columnIndex);
        checkNull(columnIndex);
        checkType(columnIndex, ColumnType.UNIXTIME_MICROS);
        long micros = getLong(columnIndex);
        return TimestampUtil.microsToTimestamp(micros);
    }

    @Override
    public String getVarLengthData(int columnIndex) {
        checkValidColumn(columnIndex);
        checkNull(columnIndex);
        checkType(columnIndex, ColumnType.STRING, ColumnType.VARCHAR);
        // C++ puts a Slice in rowData which is 16 bytes long for simplicity, but we only support ints.
        int offset = getOffsetForCurrentRow(columnIndex);
        int length = getOffsetForNextRow(columnIndex) - offset;
        assert offset < Integer.MAX_VALUE;
        assert length < Integer.MAX_VALUE;
        return Bytes.getString(varlenData[columnIndex].getRawArray(),
                varlenData[columnIndex].getRawOffset() + offset,
                length);
    }

    /**
     * Get a copy of the specified column's binary data.
     * @param columnIndex Column index in the schema
     * @return a byte[] with the binary data.
     * @throws IllegalArgumentException if the column is null
     * or if the type doesn't match the column's type
     * @throws IndexOutOfBoundsException if the column doesn't exist
     */
    @Override
    public byte[] getBinaryCopy(int columnIndex) {
        checkValidColumn(columnIndex);
        checkNull(columnIndex);
        // C++ puts a Slice in rowData which is 16 bytes long for simplicity,
        // but we only support ints.
        int offset = getOffsetForCurrentRow(columnIndex);
        int length = getOffsetForNextRow(columnIndex) - offset;
        assert offset < Integer.MAX_VALUE;
        assert length < Integer.MAX_VALUE;
        byte[] ret = new byte[length];
        System.arraycopy(varlenData[columnIndex].getRawArray(),
                varlenData[columnIndex].getRawOffset() + offset,
                ret, 0, length);
        return ret;
    }

    /**
     * Get the specified column's binary data.
     *
     * This doesn't copy the data and instead returns a ByteBuffer that wraps it.
     *
     * @param columnIndex Column index in the schema
     * @return a ByteBuffer with the binary data.
     * @throws IllegalArgumentException if the column is null
     * or if the type doesn't match the column's type
     * @throws IndexOutOfBoundsException if the column doesn't exist
     */
    @Override
    public ByteBuffer getBinary(int columnIndex) {
        checkValidColumn(columnIndex);
        checkNull(columnIndex);
        checkType(columnIndex, ColumnType.BINARY);
        // C++ puts a Slice in rowData which is 16 bytes long for simplicity,
        // but we only support ints.
        int offset = getOffsetForCurrentRow(columnIndex);
        int length = getOffsetForNextRow(columnIndex) - offset;
        assert offset < Integer.MAX_VALUE;
        assert length < Integer.MAX_VALUE;
        return ByteBuffer.wrap(varlenData[columnIndex].getRawArray(),
                varlenData[columnIndex].getRawOffset() + offset, length);
    }

    @Override
    public long getLong(int columnIndex) {
        return Bytes.getLong(this.data[columnIndex].getRawArray(),
                this.data[columnIndex].getRawOffset() + index * 8);
    }

    protected int getOffsetForCurrentRow(int columnIndex) {
        return Bytes.getInt(this.data[columnIndex].getRawArray(),
                this.data[columnIndex].getRawOffset() + index * 4);
    }

    protected int getOffsetForNextRow(int columnIndex) {
        return Bytes.getInt(this.data[columnIndex].getRawArray(),
                this.data[columnIndex].getRawOffset() + (index + 1) * 4);
    }

    /**
     * Get if the specified column is NULL
     * @param columnIndex Column index in the schema
     * @return true if the column cell is null and the column is nullable,
     * false otherwise
     * @throws IndexOutOfBoundsException if the column doesn't exist
     */
    @Override
    public boolean isNull(int columnIndex) {
        checkValidColumn(columnIndex);
        if (!schema.getColumnByIndex(columnIndex).isNullable()) {
            return false;
        }

        byte flag = Bytes.getByte(this.nonNullBitmaps[columnIndex].getRawArray(),
                this.nonNullBitmaps[columnIndex].getRawOffset() + index / 8);

        boolean nonNull = (flag & (1 << (index % 8))) != 0;
        return !nonNull;
    }

    @Override
    public String toString() {
        return "ColumnarRowResult index: " + this.index;
    }
}

