package com.liang.flink.basic.cdc;

import com.liang.common.util.JsonUtils;
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
import java.util.LinkedHashMap;
import java.util.Map;

public class CanalDebeziumDeserializationSchema implements DebeziumDeserializationSchema<String> {
    private static final ZoneOffset UTC = ZoneOffset.UTC;
    private static final ZoneOffset CST = ZoneOffset.ofHours(+8);
    private static final DateTimeFormatter yyyyMMddHHmmSS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter yyyyMMdd = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) {
        Struct value = (Struct) sourceRecord.value();
        collector.collect(JsonUtils.toString(structToMap(value)));
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }

    private Map<String, Object> structToMap(Struct struct) {
        LinkedHashMap<String, Object> map = new LinkedHashMap<>();
        for (Field key : struct.schema().fields()) {
            Object value = struct.get(key);
            String name = key.name();
            String schemaName = key.schema().name();
            System.out.println(key);
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
