package org.apache.hadoop.hetu.types;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Created by xiliu on 2021/5/28
 */
public class TestStructType {
    @Test
    public void testStructTypeBuilder() {
        StructType structType = new StructType();
        structType.add("ID", new LongType(), false);
        structType.add("NAME", new StringType(), true, "用户 ID");
        System.out.println(structType.treeString());
        Assert.assertTrue(structType.nameToIndex().size() == 2);
        Assert.assertTrue(structType.getFieldIndex("NAME") == 1);
        Assert.assertTrue(structType.getStructType(Arrays.asList("NAME").stream().collect(Collectors.toSet())).fieldNamesSet().size() == 1);
    }
}
