package com.liang.flink.basic.cdc;

import cn.hutool.core.util.ObjUtil;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.liang.common.util.JsonUtils;
import io.debezium.time.Date;
import io.debezium.time.Timestamp;
import io.debezium.time.ZonedTimestamp;
import lombok.extern.slf4j.Slf4j;
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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@SuppressWarnings("unchecked")
public class CanalDebeziumDeserializationSchema implements DebeziumDeserializationSchema<FlatMessage> {
    private static final ZoneOffset UTC = ZoneOffset.ofHours(0);
    private static final ZoneOffset CST = ZoneOffset.ofHours(+8);
    private static final DateTimeFormatter yyyyMMddHHmmSS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter yyyyMMdd = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<FlatMessage> collector) {
        Struct recordValue = (Struct) sourceRecord.value();
        Map<String, Object> debeziumMap = structToMap(recordValue);
        try {
            FlatMessage flatMessage = debeziumMapToFlatMessage(debeziumMap);
            collector.collect(flatMessage);
        } catch (Exception e) {
            log.error("cdc to canal error, debezium map: {}", JsonUtils.toString(debeziumMap));
        }
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
            else if (value instanceof Integer && Date.SCHEMA_NAME.equals(schemaName)) {
                LocalDate date = LocalDate.ofEpochDay(((Integer) value));
                map.put(name, date.format(yyyyMMdd));
            }
            //datetime
            else if (value instanceof Long && Timestamp.SCHEMA_NAME.equals(schemaName)) {
                LocalDateTime datetime = LocalDateTime.ofEpochSecond(((Long) value) / 1000, 0, UTC);
                map.put(name, datetime.format(yyyyMMddHHmmSS));
            }
            // timestamp
            else if (value instanceof String && ZonedTimestamp.SCHEMA_NAME.equals(schemaName)) {
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

    private FlatMessage debeziumMapToFlatMessage(Map<String, Object> debeziumMap) {
        Map<String, String> before = (Map<String, String>) debeziumMap.get("before");
        Map<String, String> after = (Map<String, String>) debeziumMap.get("after");
        Map<String, Object> source = (Map<String, Object>) debeziumMap.get("source");
        FlatMessage flatMessage = new FlatMessage();
        flatMessage.setDatabase((String) source.get("db"));
        flatMessage.setTable((String) source.get("table"));
        flatMessage.setPkNames(Collections.singletonList("id"));
        flatMessage.setIsDdl(false);
        switch ((String) debeziumMap.get("op")) {
            case "c":
                flatMessage.setType(CanalEntry.EventType.INSERT.name());
                flatMessage.setData(Collections.singletonList(after));
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
                break;
            default:
                throw new RuntimeException();
        }
        flatMessage.setEs(Long.parseLong((String) source.get("ts_ms")));
        flatMessage.setTs(Long.parseLong((String) debeziumMap.get("ts_ms")));
        return flatMessage;
    }
}