package com.liang.common.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DorisSchema implements Serializable {
    public static final String DEFAULT_UNIQUE_DELETE_ON = "__DORIS_DELETE_SIGN__ = 1";
    public static final String DEFAULT_UNIQUE_ORDER_BY = "__DORIS_SEQUENCE_COL__";

    private String database;
    private String tableName;
    private String uniqueDeleteOn;
    private String uniqueOrderBy;
    private List<String> derivedColumns;
}
