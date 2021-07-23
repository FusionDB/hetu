package org.apache.hadoop.hetu;

import org.apache.hadoop.hetu.types.StructType;

import java.util.List;

/**
 * Created by xiliu on 2021/5/28
 */
public class GenericRowWithSchema extends GenericRow {
    public GenericRowWithSchema(List<Object> values, StructType schema) {
        this.objectList = values;
        this.schema = schema;
    }

    public int fieldIndex(String name) {
        return schema.fieldIndex(name);
    }
}
