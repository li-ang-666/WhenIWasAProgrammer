package com.liang.flink.dto;

import com.alibaba.otter.canal.client.CanalMessageDeserializer;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.FlatMessage;
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
import java.util.function.Consumer;

@Data
@Slf4j
@NoArgsConstructor
public class BatchCanalBinlog implements Serializable {
    private final List<SingleCanalBinlog> singleCanalBinlogs = new ArrayList<>();

    public BatchCanalBinlog(byte[] kafkaRecordValue) {
        try {
            if (kafkaRecordValue == null || kafkaRecordValue.length < 2) {
                return;
            }
            if (kafkaRecordValue[0] == (byte) '{' && kafkaRecordValue[1] == (byte) '\"') {
                parseJsonMessage(kafkaRecordValue);
            } else {
                parseProtobufMessage(kafkaRecordValue);
            }
        } catch (Exception e) {
            log.error("parse canal binlog error, kafka message: {}", new String(kafkaRecordValue, StandardCharsets.UTF_8), e);
        }
    }

    public void forEach(Consumer<SingleCanalBinlog> consumer) {
        singleCanalBinlogs.forEach(consumer);
    }

    public int size() {
        return this.singleCanalBinlogs.size();
    }

    private void parseJsonMessage(byte[] kafkaRecordValue) {
        String jsonMessage = new String(kafkaRecordValue, StandardCharsets.UTF_8);
        FlatMessage flatMessage = JsonUtils.parseJsonObj(jsonMessage, FlatMessage.class);
        CanalEntry.EventType eventType = CanalEntry.EventType.valueOf(flatMessage.getType());
        boolean isDdl = flatMessage.getIsDdl();
        String sql = String.valueOf(flatMessage.getSql());
        // 排除非DML
        if ((eventType != CanalEntry.EventType.INSERT && eventType != CanalEntry.EventType.UPDATE && eventType != CanalEntry.EventType.DELETE) || isDdl) {
            log.warn("type: {}, isDdl: {}, sql: {}", eventType, isDdl, sql);
            return;
        }
        // 生成SingleCanalBinlog
        String db = flatMessage.getDatabase();
        String tb = flatMessage.getTable();
        long executeTime = flatMessage.getEs();
        List<Map<String, String>> data = flatMessage.getData();
        List<Map<String, String>> old = flatMessage.getOld();
        for (int i = 0; i < data.size(); i++) {
            Map<String, String> columnMap = data.get(i);
            SingleCanalBinlog singleCanalBinlog;
            if (eventType == CanalEntry.EventType.INSERT) {
                singleCanalBinlog = new SingleCanalBinlog(db, tb, executeTime, eventType, new LinkedHashMap<>(), new LinkedHashMap<>(columnMap));
            } else if (eventType == CanalEntry.EventType.UPDATE) {
                Map<String, String> oldColumnMapPart = old.get(i);
                Map<String, String> oldColumnMapAll = new LinkedHashMap<>(columnMap);
                oldColumnMapAll.putAll(oldColumnMapPart);
                singleCanalBinlog = new SingleCanalBinlog(db, tb, executeTime, eventType, new LinkedHashMap<>(oldColumnMapAll), new LinkedHashMap<>(columnMap));
            } else {
                singleCanalBinlog = new SingleCanalBinlog(db, tb, executeTime, eventType, new LinkedHashMap<>(columnMap), new LinkedHashMap<>());
            }
            singleCanalBinlogs.add(singleCanalBinlog);
        }
    }

    /*
     * Entry
     *   Header
     *     logfileName    [binlog文件名]
     *     logfileOffset  [binlog position]
     *     executeTime    [binlog里记录变更发生的时间戳,精确到秒]
     *     schemaName
     *     tableName
     *     eventType      [insert/update/delete类型]
     *   entryType        [事务头BEGIN/事务尾END/数据ROWDATA]
     *   storeValue       [byte数据,可展开，对应的类型为RowChange]
     * --------------------------------------------------------------
     * RowChange:
     *   isDdl            [是否是ddl变更操作，比如create table/drop table]
     *   rowDataList      [具体insert/update/delete的变更数据，可为多条，1个binlog event事件可对应多条变更，比如批处理]
     *   beforeColumns    [Column类型的数组，变更前的数据字段]
     *   afterColumns     [Column类型的数组，变更后的数据字段]
     * --------------------------------------------------------------
     * Column:
     *   index
     *   sqlType          [jdbc type]
     *   name             [column name]
     *   isKey            [是否为主键]
     *   updated          [是否发生过变更]
     *   isNull           [值是否为null]
     *   value            [具体的内容，注意为string文本]
     */
    private void parseProtobufMessage(byte[] kafkaRecordValue) {
        Message message = CanalMessageDeserializer.deserializer(kafkaRecordValue);
        // 判断entries
        if (message.getId() == -1L || message.getEntries().isEmpty()) {
            return;
        }
        // 遍历entries, 判断每一个entry
        for (CanalEntry.Entry entry : message.getEntries()) {
            if (entry.getEntryType() != CanalEntry.EntryType.ROWDATA) {
                continue;
            }
            // 解析出rowChange, 判断每一个row
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
            // 排除非DML
            if ((eventType != CanalEntry.EventType.INSERT && eventType != CanalEntry.EventType.UPDATE && eventType != CanalEntry.EventType.DELETE) || isDdl) {
                log.warn("type: {}, isDdl: {}, sql: {}", eventType, isDdl, sql);
                continue;
            }
            // 生成SingleCanalBinlog
            CanalEntry.Header header = entry.getHeader();
            String db = header.getSchemaName();
            String tb = header.getTableName();
            long executeTime = header.getExecuteTime();
            List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
            for (CanalEntry.RowData rowData : rowDatasList) {
                Map<String, String> beforeColumnMap = columnListToColumnMap(rowData.getBeforeColumnsList());
                Map<String, String> afterColumnMap = columnListToColumnMap(rowData.getAfterColumnsList());
                SingleCanalBinlog singleCanalBinlog;
                if (eventType == CanalEntry.EventType.INSERT) {
                    singleCanalBinlog = new SingleCanalBinlog(db, tb, executeTime, eventType, new LinkedHashMap<>(), new LinkedHashMap<>(afterColumnMap));
                } else if (eventType == CanalEntry.EventType.UPDATE) {
                    singleCanalBinlog = new SingleCanalBinlog(db, tb, executeTime, eventType, new LinkedHashMap<>(beforeColumnMap), new LinkedHashMap<>(afterColumnMap));
                } else {
                    singleCanalBinlog = new SingleCanalBinlog(db, tb, executeTime, eventType, new LinkedHashMap<>(beforeColumnMap), new LinkedHashMap<>());
                }
                singleCanalBinlogs.add(singleCanalBinlog);
            }
        }
    }

    private Map<String, String> columnListToColumnMap(List<CanalEntry.Column> columnList) {
        LinkedHashMap<String, String> columnMap = new LinkedHashMap<>(columnList.size());
        for (CanalEntry.Column column : columnList) {
            String columnName = column.getName();
            String columnValue = column.getIsNull() ? null : column.getValue();
            columnMap.put(columnName, columnValue);
        }
        return columnMap;
    }
}
