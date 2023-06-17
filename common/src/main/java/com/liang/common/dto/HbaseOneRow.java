package com.liang.common.dto;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@Data
public class HbaseOneRow implements Serializable {
    private final HbaseSchema schema;
    private final String rowKey;
    private final Map<String, Object> columnMap;

    public HbaseOneRow(HbaseSchema schema, String rowKey) {
        this.schema = schema;
        this.rowKey = schema.isRowKeyReverse() ? StringUtils.reverse(rowKey) : rowKey;
        columnMap = new HashMap<>();
    }

    public HbaseOneRow(HbaseSchema schema, String rowKey, Map<String, Object> columnMap) {
        this.schema = schema;
        this.rowKey = schema.isRowKeyReverse() ? StringUtils.reverse(rowKey) : rowKey;
        this.columnMap = columnMap;
    }

    public HbaseOneRow put(String column, Object value) {
        columnMap.put(column, value);
        return this;
    }

    public HbaseOneRow putAll(Map<String, Object> map) {
        map.forEach(this::put);
        return this;
    }
}
