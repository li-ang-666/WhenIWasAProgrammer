package com.liang.common.service.storage.parquet;

import com.liang.common.dto.DorisOneRow;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Slf4j
public class FsParquetWriter {
    private final String path;
    private final Map<String, ReadableSchema> formatInfo = new HashMap<>();
    private final Schema schema;
    private ParquetWriter<GenericRecord> writer;


    public FsParquetWriter(String path, Map<String, String> descInfo) {
        this.path = path;
        val fields = SchemaBuilder.record(DorisOneRow.class.getSimpleName()).fields();
        descInfo.forEach((columnName, mysqlType) -> {
            ReadableSchema readableSchema = formatType(columnName, mysqlType);
            formatInfo.put(columnName, readableSchema);
            fields.name(columnName)
                    .type(readableSchema.getSchema())
                    .withDefault(null);
        });
        schema = fields.endRecord();
    }

    private static int computeMinBytesForDecimalPrecision(int precision) {
        int numBytes = 1;
        while (Math.pow(2.0, 8 * numBytes - 1) < Math.pow(10.0, precision)) {
            numBytes += 1;
        }
        return numBytes;
    }

    public void write(Map<String, Object> columnMap) {
        try {
            GenericData.Record record = getRecord(columnMap);
            ParquetWriter<GenericRecord> writer = (this.writer != null) ? this.writer : getWriter();
            writer.write(record);
        } catch (Exception e) {
            String msg = "FsParquetWriter write error";
            log.error(msg, e);
            throw new RuntimeException(msg, e);
        }
    }

    private GenericData.Record getRecord(Map<String, Object> columnMap) {
        GenericRecordBuilder recordBuilder = new GenericRecordBuilder(schema);
        columnMap.forEach((k, v) -> {
            if (formatInfo.containsKey(k)) {
                recordBuilder.set(k, formatValue(k, v));
            }
        });
        return recordBuilder.build();
    }

    private Object formatValue(String k, Object v) {
        if (v == null) {
            return null;
        }
        ReadableSchema readableSchema = formatInfo.get(k);
        if (readableSchema instanceof LongSchema) {
            return Long.parseLong(String.valueOf(v));
        }
        if (readableSchema instanceof DecimalSchema) {
            return new BigDecimal(String.valueOf(v)).setScale(((DecimalSchema) readableSchema).getScale(), RoundingMode.DOWN);
        }
        if (readableSchema instanceof StringSchema) {
            return String.valueOf(v);
        }
        return null;
    }

    private ParquetWriter<GenericRecord> getWriter() {
        try {
            return AvroParquetWriter.<GenericRecord>builder(new Path(path + UUID.randomUUID()))
                    .withSchema(schema)
                    .withDataModel(new GenericData() {{
                        addLogicalTypeConversion(new Conversions.DecimalConversion());
                    }})
                    .build();
        } catch (Exception e) {
            String msg = "FsParquetWriter getWriter error";
            log.error(msg, e);
            throw new RuntimeException(msg, e);
        }
    }

    private ReadableSchema formatType(String columnName, String mysqlType) {
        // 整型
        if (mysqlType.contains("int")) {
            if (mysqlType.contains("bigint") && mysqlType.contains("unsigned")) {
                return getDecimalSchema(columnName, 20, 0);
            } else {
                return new LongSchema().setSchema(Schema.create(Schema.Type.LONG));
            }
        }
        // 精确浮点
        else if (mysqlType.contains("decimal")) {
            int precision = Integer.parseInt(mysqlType.replaceAll(".*?(\\d+).*?(\\d+).*", "$1"));
            int scale = Integer.parseInt(mysqlType.replaceAll(".*?(\\d+).*?(\\d+).*", "$2"));
            return getDecimalSchema(columnName, precision, scale);
        }
        // 其他
        else {
            return new StringSchema().setSchema(Schema.create(Schema.Type.STRING));
        }
    }

    private ReadableSchema getDecimalSchema(String columnName, int precision, int scale) {
        if (precision > 38) {
            return new StringSchema().setSchema(Schema.create(Schema.Type.STRING));
        }
        Schema schema = (precision > 18)
                ? Schema.create(Schema.Type.BYTES)
                : Schema.createFixed(columnName, null, null, computeMinBytesForDecimalPrecision(precision));
        return new DecimalSchema()
                .setPrecision(precision)
                .setScale(scale)
                .setSchema(LogicalTypes.decimal(precision, scale).addToSchema(schema));
    }

    @Setter
    @Getter
    @Accessors(chain = true)
    private static class ReadableSchema {
        protected Schema schema;
    }

    private static final class StringSchema extends ReadableSchema {
    }


    private static final class LongSchema extends ReadableSchema {
    }

    @Getter
    @Setter
    @Accessors(chain = true)
    private static final class DecimalSchema extends ReadableSchema {
        private int precision;
        private int scale;
    }
}
