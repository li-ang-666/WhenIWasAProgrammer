package com.liang.common.dto;

import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Data
public class CanalBinlog implements Serializable {
    private String database;
    private String table;
    private Long ts;
    private String type;
    private List<Map<String, Object>> data;
}
