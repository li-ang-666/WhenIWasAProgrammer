package com.liang.flink.dto;

import com.alibaba.otter.canal.protocol.CanalEntry;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.util.Map;

/**
 * <P>由 BatchCanalBinlog 解析出的 SingleCanalBinlog
 * <P>type 只有INSERT、UPDATE、DELETE 三种
 */
@Data
@AllArgsConstructor
public class SingleCanalBinlog implements Serializable {
    private String database;
    private String table;
    private long executeMilliseconds;
    private CanalEntry.EventType eventType;
    private Map<String, Object> columnMap;
}
