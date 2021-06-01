package org.apache.hadoop.hetu;

import java.util.List;

/**
 * Created by xiliu on 2021/5/28
 */
public interface IRow {
    public List<Object> unapplySeq(Row row);

    public Row apply(Object... values);

    public Row fromSeq(List<Object> values);

    public Row fromTuple();

    public Row merge(Row... rows);

    public Integer length();

    public Object get(int i);

    public List<Object> toSeq();
}
