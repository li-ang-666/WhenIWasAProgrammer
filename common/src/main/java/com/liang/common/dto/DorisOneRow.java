package com.liang.common.dto;

import lombok.Data;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@Data
public class DorisOneRow implements Serializable {
    private final DorisSchema schema;
    private final Map<String, Object> columnMap;

    public DorisOneRow(DorisSchema schema) {
        this.schema = schema;
        columnMap = new HashMap<>();
    }

    public DorisOneRow(DorisSchema schema, Map<String, Object> columnMap) {
        this.schema = schema;
        this.columnMap = columnMap;
    }

    public DorisOneRow put(String column, Object value) {
        columnMap.put(column, value);
        return this;
    }

    public DorisOneRow putAll(Map<String, Object> map) {
        map.forEach(this::put);
        return this;
    }
}
