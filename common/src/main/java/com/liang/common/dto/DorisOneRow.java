package com.liang.common.dto;

import lombok.Data;

import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;

@Data
public class DorisOneRow implements Serializable {
    private DorisSchema schema;
    private Map<String, Object> columnMap;

    public DorisOneRow(DorisSchema schema) {
        this.schema = schema;
        this.columnMap = new TreeMap<>();
    }

    public DorisOneRow(DorisSchema schema, Map<String, Object> columnMap) {
        this.schema = schema;
        this.columnMap = new TreeMap<>(columnMap);
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
