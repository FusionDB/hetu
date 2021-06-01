package org.apache.hadoop.hetu.types;

import org.apache.hadoop.hetu.util.StandardTypes;

/**
 * Created by xiliu on 2021/5/28
 */
public class LongType extends DataType {
    @Override
    public String getDisplayName() {
        return StandardTypes.BIGINT;
    }
}
