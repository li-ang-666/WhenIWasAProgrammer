package com.liang.flink.dto;

import com.alibaba.otter.canal.protocol.CanalEntry;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Map;

/**
 * 由 BatchCanalBinlog 解析出的 SingleCanalBinlog
 * type 只有INSERT、UPDATE、DELETE 三种
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class SingleCanalBinlog implements Serializable {
    private String database;
    private String table;
    private long executeMilliseconds;
    private CanalEntry.EventType eventType;
    private Map<String, Object> columnMap;
    private Map<String, Object> beforeColumnMap;
    private Map<String, Object> afterColumnMap;
}
