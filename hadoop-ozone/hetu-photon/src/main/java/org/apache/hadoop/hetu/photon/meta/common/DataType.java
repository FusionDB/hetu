package org.apache.hadoop.hetu.photon.meta.common;

/**
 * Created by xiliu on 2021/8/17
 */
public enum DataType {
    UNKNOWN_DATA,
    UINT8,
    INT8,
    UINT16,
    INT16,
    UINT32,
    INT32,
    UINT64,
    INT64,
    STRING,
    BOOL,
    FLOAT,
    DOUBLE,
    BINARY,
    UNIXTIME_MICROS,
    INT128,
    DECIMAL32,
    DECIMAL64,
    DECIMAL128,
    IS_DELETED, // virtual column; not a real data type
    VARCHAR,
    DATE;
}
