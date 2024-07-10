package com.liang.flink.basic.cdc;

import cn.hutool.core.util.ObjUtil;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.FlatMessage;
import io.debezium.time.Date;
import io.debezium.time.Timestamp;
import io.debezium.time.ZonedTimestamp;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

@SuppressWarnings("unchecked")
public class CanalDebeziumDeserializationSchema implements DebeziumDeserializationSchema<FlatMessage> {
    private static final ZoneOffset UTC = ZoneOffset.UTC;
    private static final ZoneOffset CST = ZoneOffset.ofHours(+8);
    private static final DateTimeFormatter yyyyMMddHHmmSS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter yyyyMMdd = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<FlatMessage> collector) {
        FlatMessage flatMessage = new FlatMessage();
        Struct recordValue = (Struct) sourceRecord.value();
        Map<String, Object> debeziumMap = structToMap(recordValue);
        Map<String, String> before = (Map<String, String>) debeziumMap.getOrDefault("before", new LinkedHashMap<String, String>());
        Map<String, String> after = (Map<String, String>) debeziumMap.getOrDefault("after", new LinkedHashMap<String, String>());
        Map<String, Object> source = (Map<String, Object>) debeziumMap.get("source");
        flatMessage.setId(0);
        flatMessage.setDatabase((String) source.get("db"));
        flatMessage.setTable((String) source.get("table"));
        flatMessage.setPkNames(new ArrayList<>());
        flatMessage.setIsDdl(false);
        switch ((String) debeziumMap.get("op")) {
            case "c":
                flatMessage.setType(CanalEntry.EventType.INSERT.name());
                flatMessage.setData(Collections.singletonList(after));
                flatMessage.setOld(new ArrayList<>());
                break;
            case "u":
                flatMessage.setType(CanalEntry.EventType.UPDATE.name());
                flatMessage.setData(Collections.singletonList(after));
                Map<String, String> old = before.entrySet().stream()
                        .filter(entry -> ObjUtil.notEqual(entry.getValue(), after.get(entry.getKey())))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                flatMessage.setOld(Collections.singletonList(old));
                break;
            case "d":
                flatMessage.setType(CanalEntry.EventType.DELETE.name());
                flatMessage.setData(Collections.singletonList(before));
                flatMessage.setOld(new ArrayList<>());
                break;
        }
        flatMessage.setEs(Long.parseLong((String) source.get("ts_ms")));
        flatMessage.setTs(Long.parseLong((String) debeziumMap.get("ts_ms")));
        flatMessage.setSql("");
        flatMessage.setSqlType(new HashMap<>());
        flatMessage.setMysqlType(new HashMap<>());
        flatMessage.setGtid((String) source.get("gtid"));
        collector.collect(flatMessage);
    }

    @Override
    public TypeInformation<FlatMessage> getProducedType() {
        return TypeInformation.of(FlatMessage.class);
    }

    private Map<String, Object> structToMap(Struct struct) {
        LinkedHashMap<String, Object> map = new LinkedHashMap<>();
        for (Field key : struct.schema().fields()) {
            Object value = struct.get(key);
            String name = key.name();
            String schemaName = key.schema().name();
            // null
            if (value == null) {
                map.put(name, null);
            }
            // struct
            else if (value instanceof Struct) {
                map.put(name, structToMap((Struct) value));
            }
            // date
            else if (Date.SCHEMA_NAME.equals(schemaName) && value instanceof Integer) {
                LocalDate date = LocalDate.ofEpochDay(((Integer) value));
                map.put(name, date.format(yyyyMMdd));
            }
            //datetime
            else if (Timestamp.SCHEMA_NAME.equals(schemaName) && value instanceof Long) {
                LocalDateTime datetime = LocalDateTime.ofEpochSecond(((Long) value) / 1000, 0, UTC);
                map.put(name, datetime.format(yyyyMMddHHmmSS));
            }
            // timestamp
            else if (ZonedTimestamp.SCHEMA_NAME.equals(schemaName) && value instanceof String) {
                String timestamp = LocalDateTime.ofInstant(Instant.parse((String) value), CST).format(yyyyMMddHHmmSS);
                map.put(name, timestamp);
            }
            // string
            else {
                map.put(name, value.toString());
            }
        }
        return map;
    }
}
