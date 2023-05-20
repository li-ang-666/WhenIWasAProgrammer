package com.liang.flink.dto;

import com.alibaba.otter.canal.protocol.CanalEntry;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.util.Map;

@Data
@AllArgsConstructor
public class SingleCanalBinlog implements Serializable {
    private String database;
    private String table;
    private long executeTime;
    private CanalEntry.EventType eventType;
    private Map<String, Object> columnMap;
}
