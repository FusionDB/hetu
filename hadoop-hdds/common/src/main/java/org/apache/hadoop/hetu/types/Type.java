package org.apache.hadoop.hetu.types;

/**
 * Created by xiliu on 2021/5/27
 */
public interface Type {
    String getDisplayName();

    boolean acceptsType(String displayName);
}
