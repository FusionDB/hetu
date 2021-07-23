package org.apache.hadoop.ozone.hm.meta.table;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ColumnSchemaProto;
import java.util.Objects;

/**
 * Created by xiliu on 2021/4/6
 */
public class ColumnSchema {
    private String columnType;
    private String columnName;
    private String dataType;
    private int ordinalPosition;
    private String columnDefault;
    private String extra;
    private String columnComment;
    private boolean isNullable;

    public ColumnSchema(String columnName,
                        String columnType,
                        String dataType,
                        int ordinalPosition,
                        String columnDefault,
                        String extra,
                        String columnComment,
                        boolean isNullable) {
        this.columnComment = columnComment;
        this.columnDefault = columnDefault;
        this.columnName = columnName;
        this.columnType = columnType;
        this.dataType = dataType;
        this.ordinalPosition = ordinalPosition;
        this.extra = extra;
        this.isNullable = isNullable;
    }

    public static ColumnSchemaProto toProtobuf(ColumnSchema columnSchema) {
        ColumnSchemaProto.Builder builder = ColumnSchemaProto.newBuilder()
                .setColumnType(columnSchema.getColumnType())
                .setColumnName(columnSchema.getColumnName())
                .setDataType(columnSchema.getDataType())
                .setOrdinalPosition(columnSchema.getOrdinalPosition())
                .setColumnDefault(columnSchema.getColumnDefault())
                .setExtra(columnSchema.getExtra())
                .setColumnComment(columnSchema.getColumnComment())
                .setIsNullable(columnSchema.isNullable());
        return builder.build();
    }

    public static ColumnSchema fromProtobuf(ColumnSchemaProto columnSchemaProto) {
        return new ColumnSchema(columnSchemaProto.getColumnName(),
                columnSchemaProto.getColumnType(),
                columnSchemaProto.getDataType(),
                columnSchemaProto.getOrdinalPosition(),
                columnSchemaProto.getColumnDefault(),
                columnSchemaProto.getExtra(),
                columnSchemaProto.getColumnComment(),
                columnSchemaProto.getIsNullable());
    }

    public String getColumnType() {
        return columnType;
    }

    public void setColumnType(String columnType) {
        this.columnType = columnType;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public int getOrdinalPosition() {
        return ordinalPosition;
    }

    public void setOrdinalPosition(int ordinalPosition) {
        this.ordinalPosition = ordinalPosition;
    }

    public String getColumnDefault() {
        return columnDefault;
    }

    public void setColumnDefault(String columnDefault) {
        this.columnDefault = columnDefault;
    }

    public String getExtra() {
        return extra;
    }

    public void setExtra(String extra) {
        this.extra = extra;
    }

    public String getColumnComment() {
        return columnComment;
    }

    public void setColumnComment(String columnComment) {
        this.columnComment = columnComment;
    }

    public boolean isNullable() {
        return isNullable;
    }

    public void setNullable(boolean nullable) {
        isNullable = nullable;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ColumnSchema that = (ColumnSchema) o;
        return isNullable == that.isNullable &&
                columnType.equals(that.columnType) &&
                columnName.equals(that.columnName) &&
                dataType.equals(that.dataType) &&
                ordinalPosition == that.ordinalPosition &&
                columnDefault.equals(that.columnDefault) &&
                extra.equals(that.extra) &&
                columnComment.equals(that.columnComment);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnType, columnName, dataType,
                ordinalPosition, columnDefault, extra,
                columnComment, isNullable);
    }

    @Override
    public String toString() {
        return "ColumnSchema{" +
                "columnType='" + columnType + '\'' +
                ", columnName='" + columnName + '\'' +
                ", dataType='" + dataType + '\'' +
                ", ordinalPosition='" + ordinalPosition + '\'' +
                ", columnDefault='" + columnDefault + '\'' +
                ", extra='" + extra + '\'' +
                ", columnComment='" + columnComment + '\'' +
                ", isNullable=" + isNullable +
                '}';
    }
}
