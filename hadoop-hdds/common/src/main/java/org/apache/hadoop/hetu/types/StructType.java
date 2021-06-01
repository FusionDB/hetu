package org.apache.hadoop.hetu.types;

import com.google.common.collect.Streams;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by xiliu on 2021/5/27
 */
public class StructType {
    private List<StructField> fields;

    public StructType() {
        fields = new ArrayList<>();
    }

    public StructType(List<StructField> structFields) {
        this.fields = structFields;
    }

    public String[] fieldNames() {
        return fields.stream().map(StructField::getName).toArray(String[]::new);
    }

    public String[] names() {
        return fieldNames();
    }

    public Set<String> fieldNamesSet() {
        return Arrays.stream(fieldNames()).collect(Collectors.toSet());
    }

    public Map<String, StructField> nameToField() {
        return fields.stream().collect(Collectors.toMap(f -> f.getName(), f -> f));
    }

    public Map<String, Integer> nameToIndex() {
        Map<String, Integer> nameIndex = new ConcurrentHashMap<>();
        Streams.mapWithIndex(Stream.of(fieldNames()), (str, index) -> str + ":" + index).forEach(index -> {
            String[] splitStr = index.split(":");
            nameIndex.put(splitStr[0], Integer.valueOf(splitStr[1]));
        });
        return nameIndex;
    }

    public StructType add(StructField field) {
        fields.add(field);
        return this;
    }

    public StructType add(String name, DataType dataType, Boolean nullable, Map<String, String> metadata) {
        fields.add(new StructField(name, dataType, nullable, metadata));
        return this;
    }

    public StructType add(String name, DataType dataType, Boolean nullable) {
        return add(name, dataType, nullable, new HashMap<>());
    }

    public StructType add(String name, DataType dataType) {
        return add(name, dataType, null, new HashMap<>());
    }

    public StructType add(String name, DataType dataType, Boolean nullable, String comment) {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("comment", comment);
        return add(name, dataType, nullable, metadata);
    }

    public StructField getStructField(String name) {
        return nameToField().get(name);
    }

    public StructType getStructType(Set<String> names) {
        return new StructType(fields.stream().filter(structField -> names.contains(structField.getName())).collect(Collectors.toList()));
    }

    public int fieldIndex(String name) {
        return nameToIndex().get(name);
    }

    public int getFieldIndex(String name) {
        return nameToIndex().get(name);
    }

    public String treeString() {
        StringBuilder builder = new StringBuilder();
        builder.append("root\n");
        String prefix = " |";
        fields.stream().forEach(field -> field.buildFormattedString(prefix, builder));
        return builder.toString();
    }

    @Override
    public String toString() {
        return "StructType{" +
                "fields=" + fields +
                '}';
    }
}
