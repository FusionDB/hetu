package org.apache.hadoop.hetu;

import java.util.Arrays;
import java.util.List;

/**
 * Created by xiliu on 2021/5/28
 */
public class GenericRow extends Row {
    public GenericRow(List<Object> values) {
        this.objectList = values;
    }

    public GenericRow() {

    }

    @Override
    public List<Object> unapplySeq(Row row) {
        return row.objectList;
    }

    @Override
    public Row apply(Object... values) {
        Arrays.asList(values).stream().forEach(v -> objectList.add(v));
        return this;
    }

    @Override
    public Row fromSeq(List<Object> values) {
        values.stream().forEach(v -> objectList.add(v));
        return this;
    }

    @Override
    public Row fromTuple() {
        return null;
    }

    @Override
    public Row merge(Row... rows) {
        return null;
    }

    @Override
    public Integer length() {
        return objectList.size();
    }

}
