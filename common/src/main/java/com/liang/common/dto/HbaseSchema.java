package com.liang.common.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class HbaseSchema implements Serializable {
    private String namespace;
    private String tableName;
    private String columnFamily;
    private boolean rowKeyReverse;
}
