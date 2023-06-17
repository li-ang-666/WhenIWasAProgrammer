package com.liang.common.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
@AllArgsConstructor
public class DorisSchema implements Serializable {
    private String database;
    private String tableName;
    private String uniqueDeleteOn;
    private String uniqueOrderBy;
    private List<String> derivedColumns;
}
