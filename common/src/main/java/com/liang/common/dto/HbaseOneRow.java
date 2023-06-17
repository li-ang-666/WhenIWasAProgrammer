package com.liang.common.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class HbaseOneRow implements Serializable {
    private HbaseSchema schema;
    private String rowKey;
    private Map<String, Object> columnMap;

    public HbaseOneRow(HbaseSchema schema, String rowKey) {
        this.schema = schema;
        this.rowKey = schema.isRowKeyReverse() ? StringUtils.reverse(rowKey) : rowKey;
        columnMap = new HashMap<>();
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
