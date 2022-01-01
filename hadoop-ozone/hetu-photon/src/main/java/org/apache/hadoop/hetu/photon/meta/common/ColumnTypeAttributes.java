package org.apache.hadoop.hetu.photon.meta.common;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hetu.photon.proto
        .PhotonSchemaProtocolProtos
        .ColumnSchemaProto
        .ColumnTypeAttributesProto;

import java.util.Objects;

/**
 * Represents a Hetu Table column's type attributes.
 */
@org.apache.yetus.audience.InterfaceAudience.Public
@org.apache.yetus.audience.InterfaceStability.Evolving
public class ColumnTypeAttributes {

    private final boolean hasPrecision;
    private final int precision;

    private final boolean hasScale;
    private final int scale;

    private final boolean hasLength;
    private final int length;

    private ColumnTypeAttributes(boolean hasPrecision, int precision,
                                 boolean hasScale, int scale,
                                 boolean hasLength, int length) {
        this.hasPrecision = hasPrecision;
        this.precision = precision;
        this.hasScale = hasScale;
        this.scale = scale;
        this.hasLength = hasLength;
        this.length = length;
    }

    /**
     * Returns true if the precision is set;
     */
    public boolean hasPrecision() {
        return hasPrecision;
    }

    /**
     * Return the precision;
     */
    public int getPrecision() {
        return precision;
    }

    /**
     * Returns true if the scale is set;
     */
    public boolean hasScale() {
        return hasScale;
    }

    /**
     * Return the scale;
     */
    public int getScale() {
        return scale;
    }

    /**
     * Returns true if the length is set;
     */
    public boolean hasLength() {
        return hasLength;
    }

    /**
     * Returns the length;
     */
    public int getLength() {
        return length;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ColumnTypeAttributes)) {
            return false;
        }

        ColumnTypeAttributes that = (ColumnTypeAttributes) o;

        if (hasPrecision != that.hasPrecision) {
            return false;
        }
        if (precision != that.precision) {
            return false;
        }
        if (hasScale != that.hasScale) {
            return false;
        }
        if (scale != that.scale) {
            return false;
        }
        if (hasLength != that.hasLength) {
            return false;
        }
        if (length != that.length) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(hasPrecision, precision, hasScale, scale, hasLength, length);
    }

    /**
     * Return a string representation appropriate for `type`.
     * This is meant to be postfixed to the name of a primitive type to describe
     * the full type, e.g. decimal(10, 4).
     * @param type the type.
     * @return a postfix string.
     */
    public String toStringForType(ColumnType type) {
        if (type == ColumnType.DECIMAL) {
            return "(" + precision + ", " + scale + ")";
        } else if (type == ColumnType.VARCHAR) {
            return "(" + length + ")";
        } else {
            return "";
        }
    }

    @Override
    public String toString() {
        return "hasPrecision: " + hasPrecision + ", precision: " + precision +
                ", hasScale: " + hasScale + ", scale: " + scale +
                ", hasLength: " + hasLength + ", length: " + length;
    }

    /**
     * Returns new builder class that builds a OmTableArgs.
     * @return Builder
     */
    public static ColumnTypeAttributes.Builder newBuilder() {
        return new ColumnTypeAttributes.Builder();
    }

    /**
     * Builder for ColumnTypeAttributes.
     */
    @org.apache.yetus.audience.InterfaceAudience.Public
    @org.apache.yetus.audience.InterfaceStability.Evolving
    public static class Builder {

        private boolean hasPrecision;
        private int precision;
        private boolean hasScale;
        private int scale;
        private boolean hasLength;
        private int length;

        /**
         * Set the precision. Only used for Decimal columns.
         */
        public Builder precision(int precision) {
            this.hasPrecision = true;
            this.precision = precision;
            return this;
        }

        /**
         * Set the scale. Only used for Decimal columns.
         */
        public Builder scale(int scale) {
            this.hasScale = true;
            this.scale = scale;
            return this;
        }

        public Builder length(int length) {
            this.hasLength = true;
            this.length = length;
            return this;
        }

        /**
         * Builds a {@link ColumnTypeAttributes} using the passed parameters.
         * @return a new {@link ColumnTypeAttributes}
         */
        public ColumnTypeAttributes build() {
            Preconditions.checkNotNull(precision);
            Preconditions.checkNotNull(scale);
            Preconditions.checkNotNull(length);
            return new ColumnTypeAttributes(hasPrecision, precision, hasScale, scale, hasLength, length);
        }
    }

    public ColumnTypeAttributesProto toProtobuf() {
        return ColumnTypeAttributesProto.newBuilder()
                .setLength(length)
                .setPrecision(precision)
                .setScale(scale)
                .build();
    }

    public static ColumnTypeAttributes fromProtobuf(ColumnTypeAttributesProto columnTypeAttributesProto) {
       ColumnTypeAttributes columnTypeAttributes = new Builder()
               .length(columnTypeAttributesProto.getLength())
               .precision(columnTypeAttributesProto.getPrecision())
               .scale(columnTypeAttributesProto.getScale())
               .build();
       return columnTypeAttributes;
    }
}