package com.liang.flink.dto;

import com.alibaba.otter.canal.client.CanalMessageDeserializer;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.liang.common.util.JsonUtils;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.*;

@Data
@Slf4j
@NoArgsConstructor
@SuppressWarnings("unchecked")
public class BatchCanalBinlog implements Serializable {
    private final List<SingleCanalBinlog> singleCanalBinlogs = new ArrayList<>();

    public BatchCanalBinlog(byte[] kafkaRecordValue) {
        if (kafkaRecordValue == null || kafkaRecordValue.length < 2) return;
        if (kafkaRecordValue[0] == (byte) '{'/* ascii = 123 */
                && kafkaRecordValue[1] == (byte) '\"'/* ascii = 34 */) {
            parseJsonMessage(kafkaRecordValue);
        } else {
            parseProtobufMessage(kafkaRecordValue);
        }
    }

    public int size() {
        return this.singleCanalBinlogs.size();
    }

    private void parseJsonMessage(byte[] kafkaRecordValue) {
        Map<String, Object> binlogMap = JsonUtils.parseJsonObj(
                new String(kafkaRecordValue, StandardCharsets.UTF_8));
        String type = String.valueOf(binlogMap.get("type"));
        CanalEntry.EventType eventType;
        switch (type) {
            case "INSERT":
                eventType = CanalEntry.EventType.INSERT;
                break;
            case "UPDATE":
                eventType = CanalEntry.EventType.UPDATE;
                break;
            case "DELETE":
                eventType = CanalEntry.EventType.DELETE;
                break;
            default:
                String isDdl = String.valueOf(binlogMap.get("isDdl"));
                String sql = String.valueOf(binlogMap.get("sql"));
                log.warn("type: {}, isDdl: {}, sql: {}", type, isDdl, sql);
                return;
        }
        String db = String.valueOf(binlogMap.get("database"));
        String tb = String.valueOf(binlogMap.get("table"));
        long executeTime = Long.parseLong(String.valueOf(binlogMap.get("es")));
        List<Map<String, Object>> data = (List<Map<String, Object>>) binlogMap.get("data");
        List<Map<String, Object>> old = (List<Map<String, Object>>) binlogMap.get("old");
        for (int i = 0; i < data.size(); i++) {
            Map<String, Object> columnMap = data.get(i);
            if (eventType == CanalEntry.EventType.INSERT) {
                singleCanalBinlogs.add(new SingleCanalBinlog(db, tb, executeTime, eventType, columnMap, new HashMap<>(), columnMap));
            } else if (eventType == CanalEntry.EventType.UPDATE) {
                Map<String, Object> oldColumnMapPart = old.get(i);
                Map<String, Object> oldColumnMapAll = new LinkedHashMap<>(columnMap);
                oldColumnMapAll.putAll(oldColumnMapPart);
                singleCanalBinlogs.add(new SingleCanalBinlog(db, tb, executeTime, eventType, columnMap, oldColumnMapAll, columnMap));
            } else {
                singleCanalBinlogs.add(new SingleCanalBinlog(db, tb, executeTime, eventType, columnMap, columnMap, new HashMap<>()));
            }
        }
    }

    private void parseProtobufMessage(byte[] kafkaRecordValue) {
        Message message = CanalMessageDeserializer.deserializer(kafkaRecordValue);
        //判断entries
        if (message.getId() == -1L || message.getEntries().isEmpty()) {
            return;
        }
        //遍历entries, 判断每一个entry
        for (CanalEntry.Entry entry : message.getEntries()) {
            if (entry.getEntryType() != CanalEntry.EntryType.ROWDATA) {
                continue;
            }
            //解析出rowChange, 判断每一个row
            CanalEntry.RowChange rowChange;
            try {
                rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                log.error("解析 entry 失败, message: {}, entry: {}", message, entry, e);
                continue;
            }
            CanalEntry.EventType eventType = rowChange.getEventType();
            boolean isDdl = rowChange.getIsDdl();
            String sql = rowChange.getSql();
            //官方case的排除
            if (eventType == CanalEntry.EventType.QUERY || isDdl) {
                log.warn("isDdl: {}, sql: {}", isDdl, sql);
                continue;
            }
            //生成SingleCanalBinlog
            CanalEntry.Header header = entry.getHeader();
            String db = header.getSchemaName();
            String tb = header.getTableName();
            long executeTime = header.getExecuteTime();
            List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
            for (CanalEntry.RowData rowData : rowDatasList) {
                Map<String, Object> beforeColumnMap = columnListToColumnMap(rowData.getBeforeColumnsList());
                Map<String, Object> afterColumnMap = columnListToColumnMap(rowData.getAfterColumnsList());
                if (eventType == CanalEntry.EventType.INSERT) {
                    singleCanalBinlogs.add(new SingleCanalBinlog(db, tb, executeTime, eventType, afterColumnMap, new HashMap<>(), afterColumnMap));
                } else if (eventType == CanalEntry.EventType.UPDATE) {
                    singleCanalBinlogs.add(new SingleCanalBinlog(db, tb, executeTime, eventType, afterColumnMap, beforeColumnMap, afterColumnMap));
                } else if (eventType == CanalEntry.EventType.DELETE) {
                    singleCanalBinlogs.add(new SingleCanalBinlog(db, tb, executeTime, eventType, beforeColumnMap, beforeColumnMap, new HashMap<>()));
                } else {
                    log.warn("singleCanalBinlog对象非增删改,type: {}, sql: {}", eventType, sql);
                }
            }
        }
    }

    private Map<String, Object> columnListToColumnMap(List<CanalEntry.Column> columnList) {
        LinkedHashMap<String, Object> columnMap = new LinkedHashMap<>(columnList.size());
        for (CanalEntry.Column column : columnList) {
            String columnName = column.getName();
            String columnValue = column.getValue();
            columnMap.put(columnName, columnValue);
        }
        return columnMap;
    }
}
