package com.liang.flink.dto;

import com.alibaba.otter.canal.client.CanalMessageDeserializer;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

@Data
@Slf4j
public class BatchCanalBinlog implements Serializable {
    private List<SingleCanalBinlog> singleCanalBinlogs;

    public int size() {
        return this.singleCanalBinlogs.size();
    }

    public BatchCanalBinlog(byte[] kafkaRecordValue) {
        for (byte b : kafkaRecordValue) {
            if ('{' == (char) b) {
                parseJsonMessage(kafkaRecordValue);
                return;
            } else if (!Character.isWhitespace((char) b)) {
                parseProtobufMessage(kafkaRecordValue);
                return;
            }
        }
    }

    private void parseJsonMessage(byte[] kafkaRecordValue) {

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
                //before columns
                List<CanalEntry.Column> beforeColumnsList = rowData.getBeforeColumnsList();
                LinkedHashMap<String, String> beforeMap = new LinkedHashMap<>();
                for (CanalEntry.Column column : beforeColumnsList) {
                    String columnName = column.getName();
                    String columnValue = column.getValue();
                    beforeMap.put(columnName, columnValue);
                }
                //after columns
                List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
                LinkedHashMap<String, String> afterMap = new LinkedHashMap<>();
                for (CanalEntry.Column column : afterColumnsList) {
                    String columnName = column.getName();
                    String columnValue = column.getValue();
                    afterMap.put(columnName, columnValue);
                }
                SingleCanalBinlog singleCanalBinlog = new SingleCanalBinlog(db, tb, executeTime, eventType, beforeMap, afterMap);
                if (!(eventType == CanalEntry.EventType.INSERT
                        || eventType == CanalEntry.EventType.UPDATE
                        || eventType == CanalEntry.EventType.DELETE)) {
                    log.warn("singleCanalBinlog对象非增删改, sql: {}, singleCanalBinlog: {}", sql, singleCanalBinlog);
                }
                if (singleCanalBinlogs == null) {
                    singleCanalBinlogs = new ArrayList<>();
                }
                singleCanalBinlogs.add(singleCanalBinlog);
            }
        }
    }
}
