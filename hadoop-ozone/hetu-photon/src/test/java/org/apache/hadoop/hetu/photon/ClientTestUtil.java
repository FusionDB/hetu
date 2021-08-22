package org.apache.hadoop.hetu.photon;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hetu.photon.meta.RuleType;
import org.apache.hadoop.hetu.photon.meta.common.ColumnType;
import org.apache.hadoop.hetu.photon.meta.common.ColumnKey;
import org.apache.hadoop.hetu.photon.meta.common.ColumnKeyType;
import org.apache.hadoop.hetu.photon.meta.table.ColumnSchema;
import org.apache.hadoop.hetu.photon.meta.table.DistributedKey;
import org.apache.hadoop.hetu.photon.meta.table.PartitionKey;
import org.apache.hadoop.hetu.photon.meta.table.Schema;
import org.apache.hadoop.hetu.photon.helpers.PartialRow;
import org.apache.hadoop.hetu.photon.meta.util.CharUtil;
import org.apache.hadoop.hetu.photon.meta.util.DateUtil;
import org.apache.hadoop.hetu.photon.meta.util.DecimalUtil;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

/**
 * Created by xiliu on 2021/8/18
 */
public class ClientTestUtil {
    public static PartialRow getPartialRowWithAllTypes() {
        Schema schema = getSchemaWithAllTypes();
        // Ensure we aren't missing any types
        assertEquals(15, schema.getColumnCount());

        PartialRow row = schema.newPartialRow();
        row.addByte("int8", (byte) 42);
        row.addShort("int16", (short) 43);
        row.addInt("int32", 44);
        row.addLong("int64", 45);
        row.addTimestamp("timestamp", new Timestamp(1234567890));
        row.addDate("date", DateUtil.epochDaysToSqlDate(0));
        row.addBoolean("bool", true);
        row.addFloat("float", 52.35F);
        row.addDouble("double", 53.35);
        row.addString("string", "fun with ütf\0");
        row.addVarchar("varchar", "árvíztűrő tükörfúrógép");
        row.addBinary("binary-array", new byte[] { 0, 1, 2, 3, 4 });
        ByteBuffer binaryBuffer = ByteBuffer.wrap(new byte[] { 5, 6, 7, 8, 9 });
        row.addBinary("binary-bytebuffer", binaryBuffer);
        row.setNull("null");
        row.addDecimal("decimal", BigDecimal.valueOf(12345, 3));
        return row;
    }

    public static Schema getSchemaWithAllTypes() {
        List<ColumnSchema> columns =
                ImmutableList.of(
                        new ColumnSchema.Builder("int8", ColumnType.INT8).build(),
                        new ColumnSchema.Builder("int16", ColumnType.INT16).build(),
                        new ColumnSchema.Builder("int32", ColumnType.INT32).build(),
                        new ColumnSchema.Builder("int64", ColumnType.INT64).build(),
                        new ColumnSchema.Builder("bool", ColumnType.BOOL).build(),
                        new ColumnSchema.Builder("float", ColumnType.FLOAT).build(),
                        new ColumnSchema.Builder("double", ColumnType.DOUBLE).build(),
                        new ColumnSchema.Builder("string", ColumnType.STRING).build(),
                        new ColumnSchema.Builder("binary-array", ColumnType.BINARY).build(),
                        new ColumnSchema.Builder("binary-bytebuffer", ColumnType.BINARY).build(),
                        new ColumnSchema.Builder("null", ColumnType.STRING).setNullable(true).build(),
                        new ColumnSchema.Builder("timestamp", ColumnType.UNIXTIME_MICROS).build(),
                        new ColumnSchema.Builder("decimal", ColumnType.DECIMAL)
                                .setTypeAttributes(DecimalUtil.typeAttributes(5, 3)).build(),
                        new ColumnSchema.Builder("varchar", ColumnType.VARCHAR)
                                .setTypeAttributes(CharUtil.typeAttributes(10)).build(),
                        new ColumnSchema.Builder("date", ColumnType.DATE).build());

        ColumnKey columnKey = new ColumnKey(ColumnKeyType.PRIMARY_KEY, Arrays.asList("int8", "int16", "int32"));
        DistributedKey distributedKey = new DistributedKey(RuleType.HASH, Arrays.asList("int8", "string"));
        PartitionKey partitionKey = new PartitionKey(RuleType.LIST, Arrays.asList("date"));
        return new Schema(columns, columnKey, distributedKey, partitionKey);
    }
}
