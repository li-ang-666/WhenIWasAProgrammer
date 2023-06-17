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
    private String database;
    private String tableName;
    private String uniqueDeleteOn; // __DORIS_DELETE_SIGN__ = 1
    private String uniqueOrderBy; // __DORIS_SEQUENCE_COL__
    private List<String> derivedColumns;
}
