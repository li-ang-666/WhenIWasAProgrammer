package com.liang.common.dto;

import com.liang.common.dto.config.HbaseSchema;
import com.liang.common.util.ConfigUtils;
import lombok.Data;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@Data
public class HbaseOneRow implements Serializable {
    private final HbaseSchema schema;
    private final String rowKey;
    private final Map<String, Object> columnMap = new HashMap<>();

    public HbaseOneRow(String schemaName, String rowKey) {
        this.schema = ConfigUtils.getConfig().getHbaseSchemas().get(schemaName);
        this.rowKey = rowKey;
    }

    public HbaseOneRow put(String column, Object value) {
        this.columnMap.put(schema.getColumnFamily() + ":" + column, value);
        return this;
    }

    public HbaseOneRow putAll(Map<String, Object> map) {
        map.forEach(this::put);
        return this;
    }
}
