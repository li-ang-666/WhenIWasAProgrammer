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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Data
@Slf4j
@NoArgsConstructor
@SuppressWarnings("unchecked")
public class BatchCanalBinlog implements Serializable {
    private final List<SingleCanalBinlog> singleCanalBinlogs = new ArrayList<>();

    public BatchCanalBinlog(byte[] kafkaRecordValue) {
        for (byte b : kafkaRecordValue) {
            if ((byte) '{' == b) {
                parseJsonMessage(kafkaRecordValue);
                return;
            } else if (!Character.isWhitespace((char) b)) {
                parseProtobufMessage(kafkaRecordValue);
                return;
            }
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
        for (Map<String, Object> row : data) {
            singleCanalBinlogs.add(new SingleCanalBinlog(db, tb, executeTime, eventType, row));
        }
    }

    private void parseProtobufMessage(byte[] kafkaRecordValue) {
        Message message = CanalMessageDeserializer.deserializer(kafkaRecordValue);
        //判断entries
        if (message.getId() == -1L || message.getEntries().size() == 0) {
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
                log.error("解析 entry 失败, entry: {}", entry, e);
                return;
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
                if (eventType == CanalEntry.EventType.INSERT || eventType == CanalEntry.EventType.UPDATE) {
                    //after columns
                    Map<String, Object> columnMap = columnListToColumnMap(rowData.getAfterColumnsList());
                    singleCanalBinlogs.add(new SingleCanalBinlog(db, tb, executeTime, eventType, columnMap));
                } else if (eventType == CanalEntry.EventType.DELETE) {
                    //before columns
                    Map<String, Object> columnMap = columnListToColumnMap(rowData.getBeforeColumnsList());
                    singleCanalBinlogs.add(new SingleCanalBinlog(db, tb, executeTime, eventType, columnMap));
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
