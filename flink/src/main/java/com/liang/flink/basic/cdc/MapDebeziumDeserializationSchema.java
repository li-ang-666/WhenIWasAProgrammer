package com.liang.flink.basic.cdc;

import io.debezium.time.Date;
import io.debezium.time.Timestamp;
import io.debezium.time.ZonedTimestamp;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeHint;
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
import java.util.LinkedHashMap;
import java.util.Map;

@Slf4j
public class MapDebeziumDeserializationSchema implements DebeziumDeserializationSchema<Map<String, Object>> {
    private static final ZoneOffset UTC = ZoneOffset.ofHours(0);
    private static final ZoneOffset CST = ZoneOffset.ofHours(+8);
    private static final DateTimeFormatter yyyyMMddHHmmSS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter yyyyMMdd = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<Map<String, Object>> collector) {
        Struct recordValue = (Struct) sourceRecord.value();
        Map<String, Object> debeziumMap = structToMap(recordValue);
        collector.collect(debeziumMap);
    }

    @Override
    public TypeInformation<Map<String, Object>> getProducedType() {
        return TypeInformation.of(new TypeHint<Map<String, Object>>() {
        });
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
}
