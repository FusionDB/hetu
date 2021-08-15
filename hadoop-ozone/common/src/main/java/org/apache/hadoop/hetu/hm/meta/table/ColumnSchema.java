package org.apache.hadoop.hetu.hm.meta.table;

import com.google.common.base.Preconditions;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ColumnSchemaProto;
import java.util.Objects;

/**
 * Created by xiliu on 2021/4/6
 */
public class ColumnSchema {
    private String columnType;
    private String columnName;
    private int ordinalPosition;
    private String defaultValue;
    private long desiredSize;
    private String extra;
    private String columnComment;
    private boolean isNullable;

    public ColumnSchema(String columnName,
                        String columnType,
                        int ordinalPosition,
                        String defaultValue,
                        long desiredSize,
                        String extra,
                        String columnComment,
                        boolean isNullable) {
        Preconditions.checkNotNull(columnName);
        Preconditions.checkNotNull(columnType);
        this.columnName = columnName;
        this.columnType = columnType;
        this.ordinalPosition = ordinalPosition;
        this.desiredSize = desiredSize;
        this.defaultValue = defaultValue;
        this.columnComment = columnComment;
        this.isNullable = isNullable;
        this.extra = extra;
    }

    public ColumnSchemaProto toProtobuf() {
        ColumnSchemaProto.Builder builder = ColumnSchemaProto.newBuilder()
                .setColumnName(columnName)
                .setColumnType(columnType)
                .setOrdinalPosition(ordinalPosition)
                .setDefaultValue(defaultValue)
                .setExtra(extra)
                .setDesiredSize(desiredSize)
                .setColumnComment(columnComment)
                .setIsNullable(isNullable);
        return builder.build();
    }

    public static ColumnSchema fromProtobuf(ColumnSchemaProto columnSchemaProto) {
        return new ColumnSchema(columnSchemaProto.getColumnName(),
                columnSchemaProto.getColumnType(),
                columnSchemaProto.getOrdinalPosition(),
                columnSchemaProto.getDefaultValue(),
                columnSchemaProto.getDesiredSize(),
                columnSchemaProto.getExtra(),
                columnSchemaProto.getColumnComment(),
                columnSchemaProto.getIsNullable());
    }

    public String getColumnType() {
        return columnType;
    }

    public String getColumnName() {
        return columnName;
    }

    public int getOrdinalPosition() {
        return ordinalPosition;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public long getDesiredSize() {
        return desiredSize;
    }

    public String getExtra() {
        return extra;
    }

    public String getColumnComment() {
        return columnComment;
    }

    public boolean isNullable() {
        return isNullable;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ColumnSchema that = (ColumnSchema) o;
        return isNullable == that.isNullable &&
                columnType.equals(that.columnType) &&
                columnName.equals(that.columnName) &&
                ordinalPosition == that.ordinalPosition &&
                defaultValue.equals(that.defaultValue) &&
                desiredSize == that.desiredSize &&
                extra.equals(that.extra) &&
                columnComment.equals(that.columnComment);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnType, columnName,
                ordinalPosition, defaultValue, desiredSize, extra,
                columnComment, isNullable);
    }

    @Override
    public String toString() {
        return "ColumnSchema{" +
                "columnType='" + columnType + '\'' +
                ", columnName='" + columnName + '\'' +
                ", ordinalPosition=" + ordinalPosition +
                ", defaultValue='" + defaultValue + '\'' +
                ", desiredSize=" + desiredSize +
                ", extra='" + extra + '\'' +
                ", columnComment='" + columnComment + '\'' +
                ", isNullable=" + isNullable +
                '}';
    }
}
