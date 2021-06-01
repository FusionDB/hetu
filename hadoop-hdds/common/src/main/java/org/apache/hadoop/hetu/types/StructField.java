package org.apache.hadoop.hetu.types;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by xiliu on 2021/5/27
 */
public class StructField {
    private String name;
    private DataType dataType;
    private Boolean nullable = true;
    private Map<String, String> metadata = new HashMap<>();

    public StructField(String name, DataType dataType, Boolean nullable, Map<String, String> metadata) {
        this.name = name;
        this.dataType = dataType;
        this.nullable = nullable;
        this.metadata = metadata;
    }

    public String getName() {
        return name;
    }

    public DataType getDataType() {
        return dataType;
    }

    public Boolean getNullable() {
        return nullable;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

    public void buildFormattedString(String prefix, StringBuilder builder) {
        String sf = String.format("%s -- %s: %s (nullable = %s)\n", prefix, name, dataType.getDisplayName(), nullable);
        builder.append(sf);
    }

    public StructField withComment(String comment) {
        metadata.put("comment", comment);
        return this;
    }

    public String getComment() {
        return metadata.get("comment");
    }

    @Override
    public String toString() {
        return "StructField{" +
                "name='" + name + '\'' +
                ", dataType=" + dataType +
                ", nullable=" + nullable +
                ", metadata=" + metadata +
                '}';
    }
}
