package com.liang.common.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DorisOneRow implements Serializable {
    private DorisSchema schema;
    private Map<String, Object> columnMap;

    public DorisOneRow(DorisSchema schema) {
        this.schema = schema;
        columnMap = new HashMap<>();
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
