package org.apache.hadoop.hetu.hm;

import org.apache.hadoop.hetu.photon.meta.PartitionKeyType;
import org.apache.hadoop.hetu.photon.meta.common.ColumnKey;
import org.apache.hadoop.hetu.photon.meta.common.ColumnKeyType;
import org.apache.hadoop.hetu.photon.meta.common.ColumnType;
import org.apache.hadoop.hetu.photon.meta.common.ColumnTypeAttributes;
import org.apache.hadoop.hetu.photon.meta.common.DataType;
import org.apache.hadoop.hetu.photon.meta.table.ColumnSchema;
import org.apache.hadoop.hetu.photon.meta.table.DistributedKey;
import org.apache.hadoop.hetu.photon.meta.table.PartitionKey;
import org.apache.hadoop.hetu.photon.meta.table.Schema;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.List;

/**
 * Created by xiliu on 2021/8/22
 */
public class TestBase {
    @NotNull
    public static PartitionKey getPartitionKey() {
        return new PartitionKey(PartitionKeyType.RANGE, Arrays.asList("ds"));
    }

    @NotNull
    public static DistributedKey getDistributedKey() {
        return new DistributedKey(PartitionKeyType.HASH, Arrays.asList("id"));
    }

    @NotNull
    public static List<ColumnSchema> getColumnSchemas() {
        ColumnSchema col1 = ColumnSchema.newBuilder()
                .setName("city")
                .setType(ColumnType.VARCHAR)
                .setDesiredSize(1)
                .setWireType(DataType.VARCHAR)
                .setDefaultValue("")
                .setTypeAttributes(ColumnTypeAttributes.newBuilder().length(110).build())
                .setNullable(false)
                .setComment("城市")
                .build();

        ColumnSchema col2 = ColumnSchema.newBuilder()
                .setName("id")
                .setType(ColumnType.INT64)
                .setDesiredSize(1)
                .setWireType(DataType.UINT64)
                .setDefaultValue(-1)
                .setNullable(true)
                .setComment("ID")
                .build();

        return Arrays.asList(col1, col2);
    }

    @NotNull
    public static ColumnKey getColumnKey() {
        return new ColumnKey(ColumnKeyType.PRIMARY_KEY, Arrays.asList("id"));
    }

    @NotNull
    public static Schema getSchema() {
        return new Schema(getColumnSchemas(), getColumnKey(), getDistributedKey(), getPartitionKey());
    }
}
