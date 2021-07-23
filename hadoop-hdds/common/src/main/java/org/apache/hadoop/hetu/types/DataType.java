package org.apache.hadoop.hetu.types;

import org.apache.hadoop.hetu.util.StandardTypes;

/**
 * Created by xiliu on 2021/5/27
 */
public abstract class DataType implements Type {
    @Override
    public boolean acceptsType(String displayName) {
        boolean status = false;
        switch (displayName) {
            case StandardTypes.BIGINT:
                status = true;
                break;
            case StandardTypes.VARCHAR:
                status = true;
                break;
            case StandardTypes.INTEGER:
                status = true;
                break;
            case StandardTypes.BOOLEAN:
                status = true;
                break;
        }
        return status;
    }
}
