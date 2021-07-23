package org.apache.hadoop.hetu;

import org.apache.hadoop.hetu.types.LongType;
import org.apache.hadoop.hetu.types.StringType;
import org.apache.hadoop.hetu.types.StructType;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.sun.org.apache.xalan.internal.xsltc.compiler.util.Type.Int;

/**
 * Created by xiliu on 2021/5/28
 */
public abstract class Row implements IRow, Serializable {
    public List<Object> objectList = new ArrayList<>();
    public StructType schema;

    @Override
    public List<Object> toSeq() {
        return objectList;
    }

    public Boolean isNullAt(int i) {
        return objectList.get(i) == null;
    }

    /**
     * Returns the value at position i. If the value is null, null is returned. The following
     * is a mapping between Spark SQL types and return types:
     *
     * {{{
     *   BooleanType -> java.lang.Boolean
     *   ByteType -> java.lang.Byte
     *   ShortType -> java.lang.Short
     *   IntegerType -> java.lang.Integer
     *   FloatType -> java.lang.Float
     *   DoubleType -> java.lang.Double
     *   StringType -> String
     *   DecimalType -> java.math.BigDecimal
     *
     *   DateType -> java.sql.Date
     *   TimestampType -> java.sql.Timestamp
     *
     *   BinaryType -> byte array
     *   ArrayType -> scala.collection.Seq (use getList for java.util.List)
     *   MapType -> scala.collection.Map (use getJavaMap for java.util.Map)
     *   StructType -> org.apache.spark.sql.Row
     * }}}
     */
    @Override
    public Object get(int i) {
        return objectList.get(i);
    }

    /**
     * Returns the value at position i as a primitive boolean.
     *
     * @throws ClassCastException when data type does not match.
     * @throws NullPointerException when value is null.
     */
    public Boolean getBoolean(int i) {
        return ((Boolean) get(i)).booleanValue();
    }

    /**
     * Returns the value at position i as a primitive byte.
     *
     * @throws ClassCastException when data type does not match.
     * @throws NullPointerException when value is null.
     */
     public Byte getByte(int i) {
        return Byte.valueOf((Byte) get(i));
     }

    /**
     * Returns the value at position i as a primitive short.
     *
     * @throws ClassCastException when data type does not match.
     * @throws NullPointerException when value is null.
     */
    public Short getShort(int i) {
        return Short.valueOf(get(i).toString());
    }

    /**
     * Returns the value at position i as a primitive int.
     *
     * @throws ClassCastException when data type does not match.
     * @throws NullPointerException when value is null.
     */
    public Integer getInt(int i) {
        return Integer.valueOf(get(i).toString());
    }

    /**
     * Returns the value at position i as a primitive long.
     *
     * @throws ClassCastException when data type does not match.
     * @throws NullPointerException when value is null.
     */
    public long getLong(int i) {
        return Long.valueOf(get(i).toString());
    }

    /**
     * Returns the value at position i as a primitive float.
     * Throws an exception if the type mismatches or if the value is null.
     *
     * @throws ClassCastException when data type does not match.
     * @throws NullPointerException when value is null.
     */
    public float getFloat(int i) {
        return Float.valueOf(get(i).toString());
    }

    /**
     * Returns the value at position i as a primitive double.
     *
     * @throws ClassCastException when data type does not match.
     * @throws NullPointerException when value is null.
     */
    public double getDouble(int i) {
        return Double.valueOf(get(i).toString());
    }

    /**
     * Returns the value at position i as a String object.
     *
     * @throws ClassCastException when data type does not match.
     */
    public String getString(int i) {
        return get(i).toString();
    }

    /**
     * Returns the value at position i of decimal type as java.math.BigDecimal.
     *
     * @throws ClassCastException when data type does not match.
     */
    public BigDecimal getDecimal(int i) {
        return BigDecimal.valueOf(getDouble(i));
    }

    /**
     * Returns the value at position i of date type as java.sql.Date.
     *
     * @throws ClassCastException when data type does not match.
     */
    public Date getDate(int i) {
        return (Date) get(i);
    }
    /**
     * Returns the value at position i of date type as java.sql.Timestamp.
     *
     * @throws ClassCastException when data type does not match.
     */
    public Timestamp getTimestamp(int i) {
        return Timestamp.valueOf(get(i).toString());
    }

    public <T> List<T> getList(int i, Class<T> clazz) {
        List<T> result = new ArrayList<T>();
        Object object = get(i);
        if (object instanceof List<?>) {
            for (Object o : (List<?>) object) {
                result.add(clazz.cast(o));
            }
        }
        return result;
    }

    public Map<?, ?> getMap(int i) {
        if (get(i) instanceof Map<?, ?>) {
            return (Map<?, ?>) get(i);
        }
        return null;
    }

    @Override
    public String toString() {
        return "Row{" +
                "objectList=" + objectList +
                ", schema=" + schema +
                '}';
    }
}
