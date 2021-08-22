package org.apache.hadoop.hetu.photon.meta.table;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hetu.photon.meta.common.ColumnKey;
import org.apache.hadoop.hetu.photon.meta.common.ColumnType;
import org.apache.hadoop.hetu.photon.meta.common.DataType;
import org.apache.hadoop.hetu.photon.helpers.Bytes;
import org.apache.hadoop.hetu.photon.helpers.PartialRow;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Created by xiliu on 2021/8/13
 */
public class Schema {
    private final ColumnKey columnKey;
    private final DistributedKey distributedKey;
    private final PartitionKey partitionKey;

    /**
     * Mapping of column index to column.
     */
    private final List<ColumnSchema> columnsByIndex;

    /**
     * The primary key columns.
     */
    private final List<ColumnSchema> keyColumns = new ArrayList<>();

    /**
     * Mapping of column name to index.
     */
    private final Map<String, Integer> columnsByName;

    /**
     * Mapping of column ID to index, or null if the schema does not have assigned column IDs.
     */
    private final Map<Integer, Integer> columnsById;

    /**
     * Mapping of column name to column ID, or null if the schema does not have assigned column IDs.
     */
    private final Map<String, Integer> columnIdByName;

    /**
     * Mapping of column index to backing byte array offset.
     */
    private final int[] columnOffsets;

    private final int varLengthColumnCount;
    private final int rowSize;
    private final boolean hasNullableColumns;

    private final int isDeletedIndex;
    private static final int NO_IS_DELETED_INDEX = -1;

    /**
     * Constructs a schema using the specified columns and IDs.
     *
     * This is not a stable API, prefer using {@link Schema} to create a new schema.
     *
     * @param columns the columns in index order
     * @param columnIds the column ids of the provided columns, or null
     * @throws IllegalArgumentException If the column ids length does not match the columns length
     */
    public Schema(List<ColumnSchema> columns,
                  List<Integer> columnIds,
                  ColumnKey columnKey,
                  DistributedKey distributedKey,
                  PartitionKey partitionKey) {
        Preconditions.checkArgument(columns.size() > 0);
        this.columnKey = columnKey;
        this.distributedKey = distributedKey;
        this.partitionKey = partitionKey;

        boolean hasColumnIds = columnIds != null;
        if (hasColumnIds && columns.size() != columnIds.size()) {
            throw new IllegalArgumentException(
                    "Schema must be constructed with all column IDs, or none.");
        }

        this.columnsByIndex = ImmutableList.copyOf(columns);
        int varLenCnt = 0;
        this.columnOffsets = new int[columns.size()];
        this.columnsByName = new HashMap<>(columns.size());
        this.columnsById = hasColumnIds ? new HashMap<>(columnIds.size()) : null;
        this.columnIdByName = hasColumnIds ? new HashMap<>(columnIds.size()) : null;
        int offset = 0;
        boolean hasNulls = false;
        int isDeletedIndex = NO_IS_DELETED_INDEX;
        // pre-compute a few counts and offsets
        for (int index = 0; index < columns.size(); index++) {
            final ColumnSchema column = columns.get(index);
            if (columnKey.getFields().contains(column.getColumnName())) {
                keyColumns.add(column);
            }

            hasNulls |= column.isNullable();
            columnOffsets[index] = offset;
            offset += column.getTypeSize();
            if (this.columnsByName.put(column.getColumnName(), index) != null) {
                throw new IllegalArgumentException(
                        String.format("Column names must be unique: %s", columns));
            }
            if (column.getColumnType() == ColumnType.STRING || column.getColumnType() == ColumnType.BINARY) {
                varLenCnt++;
            }

            if (hasColumnIds) {
                if (this.columnsById.put(columnIds.get(index), index) != null) {
                    throw new IllegalArgumentException(
                            String.format("Column IDs must be unique: %s", columnIds));
                }
                if (this.columnIdByName.put(column.getColumnName(), columnIds.get(index)) != null) {
                    throw new IllegalArgumentException(
                            String.format("Column names must be unique: %s", columnIds));
                }
            }

            // If this is the IS_DELETED virtual column, set `hasIsDeleted` and `isDeletedIndex`.
            if (column.getWireType() == DataType.IS_DELETED) {
                isDeletedIndex = index;
            }
        }

        this.varLengthColumnCount = varLenCnt;
        this.rowSize = getRowSize(this.columnsByIndex);
        this.hasNullableColumns = hasNulls;
        this.isDeletedIndex = isDeletedIndex;
    }

    /**
     * Constructs a schema using the specified columns and does some internal accounting
     *
     * @param columns the columns in index order
     */
    public Schema(List<ColumnSchema> columns,
                  ColumnKey columnKey,
                  DistributedKey distributedKey,
                  PartitionKey partitionKey) {
        this(columns, null, columnKey, distributedKey, partitionKey);
    }

    public OzoneManagerProtocolProtos.SchemaProto toProtobuf() {
        List<OzoneManagerProtocolProtos.ColumnSchemaProto> columnSchemaProtos = columnsByIndex.stream()
                .map(columnSchema -> columnSchema.toProtobuf())
                .collect(Collectors.toList());

        OzoneManagerProtocolProtos.SchemaProto builder = OzoneManagerProtocolProtos.SchemaProto.newBuilder()
                .addAllColumns(columnSchemaProtos)
                .setColumnKey(columnKey.toProtobuf())
                .setDistributedKey(distributedKey.toProtobuf())
                .setPartitionKey(partitionKey.toProtobuf())
                .build();
        return builder;
    }

    /**
     * Get the list of columns used to create this schema
     * @return list of columns
     */
    public List<ColumnSchema> getColumns() {
        return this.columnsByIndex;
    }

    /**
     * Get the count of columns with variable length (BINARY/STRING) in
     * this schema.
     * @return strings count
     */
    public int getVarLengthColumnCount() {
        return this.varLengthColumnCount;
    }

    /**
     * Get the size a row built using this schema would be
     * @return size in bytes
     */
    public int getRowSize() {
        return this.rowSize;
    }

    /**
     * Gives the size in bytes for a single row given the specified schema
     * @param columns the row's columns
     * @return row size in bytes
     */
    private static int getRowSize(List<ColumnSchema> columns) {
        int totalSize = 0;
        boolean hasNullables = false;
        for (ColumnSchema column : columns) {
            totalSize += column.getTypeSize();
            hasNullables |= column.isNullable();
        }
        if (hasNullables) {
            totalSize += Bytes.getBitSetSize(columns.size());
        }
        return totalSize;
    }

    /**
     * Get the index at which this column can be found in the backing byte array
     * @param idx column's index
     * @return column's offset
     */
    public int getColumnOffset(int idx) {
        return this.columnOffsets[idx];
    }

    /**
     * Returns true if the column exists.
     * @param columnName column to search for
     * @return true if the column exists
     */
    public boolean hasColumn(String columnName) {
        return this.columnsByName.containsKey(columnName);
    }

    /**
     * Get the index for the provided column name.
     * @param columnName column to search for
     * @return an index in the schema
     */
    public int getColumnIndex(String columnName) {
        Integer index = this.columnsByName.get(columnName);
        if (index == null) {
            throw new IllegalArgumentException(
                    String.format("Unknown column: %s", columnName));
        }
        return index;
    }

    /**
     * Get the column index of the column with the provided ID.
     * This method is not part of the stable API.
     * @param columnId the column id of the column
     * @return the column index of the column.
     */
    public int getColumnIndex(int columnId) {
        if (!hasColumnIds()) {
            throw new IllegalStateException("Schema does not have Column IDs");
        }
        Integer index = this.columnsById.get(columnId);
        if (index == null) {
            throw new IllegalArgumentException(
                    String.format("Unknown column id: %s", columnId));
        }
        return index;
    }

    /**
     * Get the column at the specified index in the original list
     * @param idx column's index
     * @return the column
     */
    public ColumnSchema getColumnByIndex(int idx) {
        return this.columnsByIndex.get(idx);
    }

    public boolean isKey(ColumnSchema columnSchema) {
        return columnKey.getFields().contains(columnSchema.getColumnName()) ? true : false;
    }

    /**
     * Get the column associated with the specified name
     * @param columnName column's name
     * @return the column
     */
    public ColumnSchema getColumn(String columnName) {
        return columnsByIndex.get(getColumnIndex(columnName));
    }

    /**
     * Get the count of columns in this schema
     * @return count of columns
     */
    public int getColumnCount() {
        return this.columnsByIndex.size();
    }

    /**
     * Get the count of columns that are part of the primary key.
     * @return count of primary key columns.
     */
    public int getPrimaryKeyColumnCount() {
        return this.keyColumns.size();
    }

    /**
     * Get the primary key columns.
     * @return the primary key columns.
     */
    public List<ColumnSchema> getkeyColumns() {
        return keyColumns;
    }

    /**
     * Get a schema that only contains the columns which are part of the key
     * @return new schema with only the keys
     */
    public Schema getRowKeyProjection() {
        return new Schema(keyColumns, columnKey, distributedKey, partitionKey);
    }

    /**
     * Tells if there's at least one nullable column
     * @return true if at least one column is nullable, else false.
     */
    public boolean hasNullableColumns() {
        return this.hasNullableColumns;
    }

    /**
     * Tells whether this schema includes IDs for columns. A schema created by a client as part of
     * table creation will not include IDs, but schemas for open tables will include IDs.
     * This method is not part of the stable API.
     *
     * @return whether this schema includes column IDs.
     */
    public boolean hasColumnIds() {
        return columnsById != null;
    }

    /**
     * Get the internal column ID for a column name.
     * @param columnName column's name
     * @return the column ID
     */
    @InterfaceAudience.Private
    @InterfaceStability.Unstable
    public int getColumnId(String columnName) {
        return columnIdByName.get(columnName);
    }

    /**
     * Creates a new partial row for the schema.
     * @return a new partial row
     */
    public PartialRow newPartialRow() {
        return new PartialRow(this);
    }

    /**
     * @return true if the schema has the IS_DELETED virtual column
     */
    @InterfaceAudience.Private
    @InterfaceStability.Unstable
    public boolean hasIsDeleted() {
        return isDeletedIndex != NO_IS_DELETED_INDEX;
    }

    /**
     * @return the index of the IS_DELETED virtual column
     * @throws IllegalStateException if no IS_DELETED virtual column exists
     */
    @InterfaceAudience.Private
    @InterfaceStability.Unstable
    public int getIsDeletedIndex() {
        Preconditions.checkState(hasIsDeleted(), "Schema doesn't have an IS_DELETED columns");
        return isDeletedIndex;
    }

    public ColumnKey getColumnKey() {
        return columnKey;
    }

    public DistributedKey getDistributedKey() {
        return distributedKey;
    }

    public PartitionKey getPartitionKey() {
        return partitionKey;
    }

    public static Schema fromProtobuf(OzoneManagerProtocolProtos.SchemaProto schemaProto) {
        List<ColumnSchema> columnSchemaList = schemaProto.getColumnsList().stream()
                .map(columnSchemaProto -> ColumnSchema.fromProtobuf(columnSchemaProto))
                .collect(Collectors.toList());
        return new Schema(columnSchemaList,
                ColumnKey.fromProtobuf(schemaProto.getColumnKey()),
                DistributedKey.fromProtobuf(schemaProto.getDistributedKey()),
                PartitionKey.fromProtobuf(schemaProto.getPartitionKey()));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Schema schema = (Schema) o;
        return varLengthColumnCount == schema.varLengthColumnCount &&
                rowSize == schema.rowSize &&
                hasNullableColumns == schema.hasNullableColumns &&
                isDeletedIndex == schema.isDeletedIndex &&
                columnKey.equals(schema.columnKey) &&
                distributedKey.equals(schema.distributedKey) &&
                partitionKey.equals(schema.partitionKey) &&
                columnsByIndex.equals(schema.columnsByIndex) &&
                keyColumns.equals(schema.keyColumns) &&
                columnsByName.equals(schema.columnsByName) &&
                Arrays.equals(columnOffsets, schema.columnOffsets);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(columnKey, distributedKey, partitionKey,
                columnsByIndex, keyColumns, columnsByName, columnsById,
                columnIdByName, varLengthColumnCount, rowSize, hasNullableColumns,
                isDeletedIndex);
        result = 31 * result + Arrays.hashCode(columnOffsets);
        return result;
    }

    @Override
    public String toString() {
        return "Schema{" +
                "columnKey=" + columnKey +
                ", distributedKey=" + distributedKey +
                ", partitionKey=" + partitionKey +
                ", columnsByIndex=" + columnsByIndex +
                ", keyColumns=" + keyColumns +
                ", columnsByName=" + columnsByName +
                ", columnsById=" + columnsById +
                ", columnIdByName=" + columnIdByName +
                ", columnOffsets=" + Arrays.toString(columnOffsets) +
                ", varLengthColumnCount=" + varLengthColumnCount +
                ", rowSize=" + rowSize +
                ", hasNullableColumns=" + hasNullableColumns +
                ", isDeletedIndex=" + isDeletedIndex +
                '}';
    }
}
