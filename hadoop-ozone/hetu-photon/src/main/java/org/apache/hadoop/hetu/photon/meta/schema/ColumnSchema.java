package org.apache.hadoop.hetu.photon.meta.schema;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hetu.photon.meta.common.ColumnType;
import org.apache.hadoop.hetu.photon.meta.common.ColumnTypeAttributes;
import org.apache.hadoop.hetu.photon.meta.common.DataType;
import org.apache.hadoop.hetu.photon.meta.util.CharUtil;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.apache.hadoop.hetu.photon.proto.PhotonSchemaProtocolProtos.ColumnSchemaProto;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Created by xiliu on 2021/4/6
 */
public class ColumnSchema {
    private ColumnType columnType;
    private String columnName;
    private final boolean nullable;
    private final Object defaultValue;
    private final int desiredSize;
    private final ColumnTypeAttributes typeAttributes;
    private final DataType wireType;
    private final int typeSize;
    private final String comment;

    private ColumnSchema(String columnName,
                        ColumnType columnType,
                        int desiredSize,
                        DataType wireType,
                        Object defaultValue,
                        ColumnTypeAttributes typeAttributes,
                        boolean nullable,
                        String comment) {
        Preconditions.checkNotNull(columnName);
        Preconditions.checkNotNull(columnType);
        this.columnName = columnName;
        this.columnType = columnType;
        this.desiredSize = desiredSize;
        this.wireType = wireType;
        this.typeSize = columnType.getSize(typeAttributes);
        this.typeAttributes = typeAttributes;
        this.nullable = nullable;
        this.defaultValue = defaultValue;
        this.comment = comment;
    }

    public ColumnSchemaProto toProtobuf() {
        ColumnSchemaProto.Builder builder = ColumnSchemaProto.newBuilder()
                .setColumnName(columnName)
                .setColumnType(columnType.name())
                .setDesiredSize(desiredSize)
                .setTypeSize(typeSize)
                .setWireType(wireType.name())
                .setColumnComment(comment)
                .setIsNullable(nullable);
        if (defaultValue != null) {
            builder.setDefaultValue(defaultValue.toString());
        }
        if (typeAttributes != null) {
            builder.setColumnTypeAttributes(typeAttributes.toProtobuf());
        }
        return builder.build();
    }

    public static ColumnSchema fromProtobuf(ColumnSchemaProto columnSchemaProto) {
        ColumnSchema.Builder builder = ColumnSchema.newBuilder()
                .setName(columnSchemaProto.getColumnName())
                .setType(ColumnType.valueOf(columnSchemaProto.getColumnType()))
                .setComment(columnSchemaProto.getColumnComment())
                .setDefaultValue(columnSchemaProto.getDefaultValue())
                .setDesiredSize(columnSchemaProto.getDesiredSize())
                .setNullable(columnSchemaProto.getIsNullable())
                .setWireType(DataType.valueOf(columnSchemaProto.getWireType()));

        if (ColumnType.valueOf(columnSchemaProto.getColumnType()).equals(ColumnType.DECIMAL)
                || ColumnType.valueOf(columnSchemaProto.getColumnType()).equals(ColumnType.VARCHAR)) {
            builder.setTypeAttributes(ColumnTypeAttributes.fromProtobuf(columnSchemaProto.getColumnTypeAttributes()));
        }
        return builder.build();
    }

    public ColumnType getColumnType() {
        return columnType;
    }

    public String getColumnName() {
        return columnName;
    }

    public boolean isNullable() {
        return nullable;
    }

    public Object getDefaultValue() {
        return defaultValue;
    }

    public int getDesiredSize() {
        return desiredSize;
    }

    public ColumnTypeAttributes getTypeAttributes() {
        return typeAttributes;
    }

    public DataType getWireType() {
        return wireType;
    }

    public int getTypeSize() {
        return typeSize;
    }

    public String getComment() {
        return comment;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ColumnSchema that = (ColumnSchema) o;
        return nullable == that.nullable &&
                desiredSize == that.desiredSize &&
                typeSize == that.typeSize &&
                columnType == that.columnType &&
                columnName.equals(that.columnName) &&
                defaultValue.equals(that.defaultValue) &&
                wireType == that.wireType &&
                comment.equals(that.comment);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnType, columnName, nullable, defaultValue, desiredSize, typeAttributes, wireType, typeSize, comment);
    }

    /**
     * Returns new builder class that builds a OmTableArgs.
     * @return Builder
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    @Override
    public String toString() {
        return "ColumnSchema{" +
                "columnType=" + columnType +
                ", columnName='" + columnName + '\'' +
                ", nullable=" + nullable +
                ", defaultValue=" + defaultValue +
                ", desiredSize=" + desiredSize +
                ", typeAttributes=" + typeAttributes +
                ", wireType=" + wireType +
                ", typeSize=" + typeSize +
                ", comment='" + comment + '\'' +
                '}';
    }

    /**
     * Builder for ColumnSchema.
     */
    @InterfaceAudience.Public
    @InterfaceStability.Evolving
    public static class Builder {
        private static final List<ColumnType> TYPES_WITH_ATTRIBUTES = Arrays.asList(ColumnType.DECIMAL,
                ColumnType.VARCHAR);
        private String name;
        private ColumnType type;
        private boolean nullable = false;
        private Object defaultValue = "";
        private int desiredSize = 0;
        private ColumnTypeAttributes typeAttributes = null;
        private DataType wireType = null;
        private String comment = "";

        public Builder() {}

        public Builder(String name, ColumnType type) {
            this.name = name;
            this.type = type;
        }

        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        public Builder setType(ColumnType type) {
            this.type = type;
            return this;
        }

        /**
         * Marks the column as allowing null values. False by default.
         * <p>
         * <strong>NOTE:</strong> the "not-nullable-by-default" behavior here differs from
         * the equivalent API in the Python and C++ clients. It also differs from the
         * standard behavior of SQL <code>CREATE TABLE</code> statements. It is
         * recommended to always specify nullability explicitly using this API
         * in order to avoid confusion.
         *
         * @param nullable a boolean that indicates if the column allows null values
         * @return this instance
         */
        public Builder setNullable(boolean nullable) {
            this.nullable = nullable;
            return this;
        }

        /**
         * Sets the default value that will be read from the column. Null by default.
         * @param defaultValue a Java object representation of the default value that's read
         * @return this instance
         */
        public Builder setDefaultValue(Object defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }

        /**
         * Set the desired block size for this column.
         *
         * This is the number of bytes of user data packed per block on disk, and
         * represents the unit of IO when reading this column. Larger values
         * may improve scan performance, particularly on spinning media. Smaller
         * values may improve random access performance, particularly for workloads
         * that have high cache hit rates or operate on fast storage such as SSD.
         *
         * Note that the block size specified here corresponds to uncompressed data.
         * The actual size of the unit read from disk may be smaller if
         * compression is enabled.
         *
         * It's recommended that this not be set any lower than 4096 (4KB) or higher
         * than 1048576 (1MB).
         * @param desiredSize the desired block size, in bytes
         * @return this instance
         * <!-- TODO: move the above info to docs -->
         */
        public Builder setDesiredSize(int desiredSize) {
            this.desiredSize = desiredSize;
            return this;
        }

        /**
         * Set the column type attributes for this column.
         */
        public Builder setTypeAttributes(ColumnTypeAttributes typeAttributes) {
            if (typeAttributes != null && !TYPES_WITH_ATTRIBUTES.contains(type)) {
                throw new IllegalArgumentException(
                        "ColumnTypeAttributes are not used on " + type + " columns");
            }
            this.typeAttributes = typeAttributes;
            return this;
        }

        /**
         * Allows an alternate {@link DataType} to override the {@link ColumnType}
         * when serializing the ColumnSchema on the wire.
         * This is useful for virtual columns specified by their type such as
         * {@link DataType#IS_DELETED}.
         */
        @InterfaceAudience.Private
        public Builder setWireType(DataType wireType) {
            this.wireType = wireType;
            return this;
        }

        /**
         * Set the comment for this column.
         */
        public Builder setComment(String comment) {
            this.comment = comment;
            return this;
        }

        /**
         * Builds a {@link ColumnSchema} using the passed parameters.
         * @return a new {@link ColumnSchema}
         */
        public ColumnSchema build() {
            Preconditions.checkNotNull(name);
            Preconditions.checkNotNull(type);
            // Set the wire type if it wasn't explicitly set.
            if (wireType == null) {
                this.wireType = type.getDataType(typeAttributes);
            }
            if (type == ColumnType.VARCHAR) {
                if (typeAttributes == null || !typeAttributes.hasLength() ||
                        typeAttributes.getLength() < CharUtil.MIN_VARCHAR_LENGTH ||
                        typeAttributes.getLength() > CharUtil.MAX_VARCHAR_LENGTH) {
                    throw new IllegalArgumentException(
                            String.format("VARCHAR's length must be set and between %d and %d",
                                    CharUtil.MIN_VARCHAR_LENGTH, CharUtil.MAX_VARCHAR_LENGTH));
                }
            }
            return new ColumnSchema(name, type,
                    desiredSize, wireType, defaultValue, typeAttributes, nullable, comment);
        }
    }
}
