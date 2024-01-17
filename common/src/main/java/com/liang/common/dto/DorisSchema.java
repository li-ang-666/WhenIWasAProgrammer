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

    private String database;
    private String tableName;
    private String uniqueDeleteOn;
    private List<String> derivedColumns;
    private String where;
}
