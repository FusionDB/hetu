package org.apache.hadoop.hetu.client;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hetu.photon.helpers.PartialRow;
import org.apache.hadoop.hetu.photon.meta.DistributedKeyType;
import org.apache.hadoop.hetu.photon.meta.PartitionKeyType;
import org.apache.hadoop.hetu.photon.meta.common.ColumnKey;
import org.apache.hadoop.hetu.photon.meta.common.ColumnKeyType;
import org.apache.hadoop.hetu.photon.meta.common.ColumnType;
import org.apache.hadoop.hetu.photon.meta.common.ColumnTypeAttributes;
import org.apache.hadoop.hetu.photon.meta.common.DataType;
import org.apache.hadoop.hetu.photon.meta.common.StorageEngine;
import org.apache.hadoop.hetu.photon.meta.table.ColumnSchema;
import org.apache.hadoop.hetu.photon.meta.table.DistributedKey;
import org.apache.hadoop.hetu.photon.meta.table.PartitionKey;
import org.apache.hadoop.hetu.photon.meta.table.Schema;
import org.apache.hadoop.hetu.photon.meta.util.CharUtil;
import org.apache.hadoop.hetu.photon.meta.util.DateUtil;
import org.apache.hadoop.hetu.photon.meta.util.DecimalUtil;
import org.apache.hadoop.ozone.OzoneConsts;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Created by xiliu on 2021/8/22
 */
public class TestHetuUtil {
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
        row.addBinary("binary-array", new byte[]{0, 1, 2, 3, 4});
        ByteBuffer binaryBuffer = ByteBuffer.wrap(new byte[]{5, 6, 7, 8, 9});
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

        ColumnKey columnKey = new ColumnKey(ColumnKeyType.PRIMARY_KEY, Arrays.asList("int8"));
        DistributedKey distributedKey = new DistributedKey(DistributedKeyType.HASH, Arrays.asList("int8"), 8);
        PartitionKey partitionKey = new PartitionKey(PartitionKeyType.HASH, Arrays.asList("date"), 3);
        return new Schema(columns, columnKey, distributedKey, partitionKey);
    }

    public TableArgs builderTableArgs(String databaseName, String tableName) {
        return TableArgs.newBuilder()
                .setDatabaseName(databaseName)
                .setTableName(tableName)
                .setSchema(getSchemaWithAllTypes())
                .setQuotaInBytes(OzoneConsts.HETU_QUOTA_RESET)
                .setQuotaInBucket(OzoneConsts.HETU_BUCKET_QUOTA_RESET)
                .setUsedBucket(0)
                .setUsedBytes(0L)
                .setBuckets(8)
                .setStorageEngine(StorageEngine.LSTORE)
                .addMetadata("key1", "value1")
                .setStorageType(StorageType.DISK)
                .build();
    }

    public PartialRow generateRowData() {
        Schema schema = generateSchema();
        // Ensure we aren't missing any types
        assertEquals(16, schema.getColumnCount());

        PartialRow row = schema.newPartialRow();
        long id = 1 + (int)(Math.random() * 1000);
        row.addLong("id", id);
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
        row.addBinary("binary-array", new byte[]{0, 1, 2, 3, 4});
        ByteBuffer binaryBuffer = ByteBuffer.wrap(new byte[]{5, 6, 7, 8, 9});
        row.addBinary("binary-bytebuffer", binaryBuffer);
        row.setNull("null");
        row.addDecimal("decimal", BigDecimal.valueOf(12345, 3));
        return row;
    }

    private static Schema generateSchema() {
        List<ColumnSchema> columns =
                ImmutableList.of(
                        new ColumnSchema.Builder("id", ColumnType.INT64).build(),
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

        ColumnKey columnKey = new ColumnKey(ColumnKeyType.PRIMARY_KEY, Arrays.asList("id"));
        DistributedKey distributedKey = new DistributedKey(DistributedKeyType.HASH, Arrays.asList("id"), 8);
        PartitionKey partitionKey = new PartitionKey(PartitionKeyType.HASH, Arrays.asList("date"), 3);
        return new Schema(columns, columnKey, distributedKey, partitionKey);
    }
}
