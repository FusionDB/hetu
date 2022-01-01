package org.apache.hadoop.hetu.types;

import org.apache.hadoop.hetu.GenericRow;
import org.apache.hadoop.hetu.GenericRowWithSchema;
import org.apache.hadoop.hetu.Row;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by xiliu on 2021/5/28
 */
public class TestRows {
    @Test
    public void TestRowWithSchema() {
        Row row = new GenericRow();
        row.apply(1, "Alex", 23, "Tsinghua University", null);
        Assert.assertTrue(row.length() == 5);
        Assert.assertEquals(row.getLong(0), 1);

        StructType structType = new StructType();
        structType.add("id", new LongType(), false);
        structType.add("name", new StringType(), true, "用户名称");
        structType.add("age", new LongType(), true, "用户年龄");
        structType.add("school", new StringType(), true, "大学");
        structType.add("city", new StringType(), true, "城市");

        GenericRowWithSchema row1 = new GenericRowWithSchema(row.toSeq(), structType);
        System.out.println();
        Assert.assertEquals(row1.get(row1.fieldIndex("name")), "Alex");
        Assert.assertTrue(row1.isNullAt(row1.fieldIndex("city")));
        Assert.assertTrue(row1.schema.names().length == 5);
        System.out.println(structType.toString());
    }
}
