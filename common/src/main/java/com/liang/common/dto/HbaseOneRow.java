package com.liang.common.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Map;

@Data
@AllArgsConstructor
public class HbaseOneRow {
    private String namespace;
    private String tableName;
    private String rowKey;
    private Map<String, Object> columnMap;
}
