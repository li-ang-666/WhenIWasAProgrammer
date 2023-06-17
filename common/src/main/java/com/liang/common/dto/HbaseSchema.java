package com.liang.common.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class HbaseSchema implements Serializable {
    private String namespace;
    private String tableName;
    private String columnFamily;
    private boolean rowKeyReverse;
}
