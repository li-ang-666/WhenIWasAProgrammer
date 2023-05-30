package com.liang.common.dto.config;

import lombok.Data;

import java.io.Serializable;

@Data
public class HbaseSchema implements Serializable {
    private String namespace;
    private String tableName;
    private String columnFamily;
}
