package com.liang.flink.dto;

import com.alibaba.otter.canal.protocol.CanalEntry;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
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
    private Map<String, Object> beforeColumnMap;
    private Map<String, Object> afterColumnMap;

    public Map<String, Object> getColumnMap() {
        if (eventType == CanalEntry.EventType.DELETE) {
            return getBeforeColumnMap();
        } else {
            return getAfterColumnMap();
        }
    }

    public String yyyyMMddHHmmss() {
        return LocalDateTime
                .ofEpochSecond(executeMilliseconds / 1000, 0, ZoneOffset.of("+8"))
                .format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
    }

    public String getExecuteDateTime() {
        return LocalDateTime
                .ofEpochSecond(executeMilliseconds / 1000, 0, ZoneOffset.of("+8"))
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    public String getExecuteDateTimeWithMilli() {
        return LocalDateTime
                .ofEpochSecond(executeMilliseconds / 1000, 0, ZoneOffset.of("+8"))
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
                + String.format(".%03d", executeMilliseconds % 1000);
    }
}
