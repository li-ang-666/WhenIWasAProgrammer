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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
public class FsParquetWriter {
    private static final int PARQUET_ROW_GROUP_SIZE = 128 * 1024 * 1024;
    private final String path;
    private final Map<String, ReadableSchema> formatInfo = new HashMap<>();
    private final Schema schema;
    private ParquetWriter<GenericRecord> writer;

    public FsParquetWriter(String sinkTableName, Map<String, String> descInfo) {
        path = "obs://hadoop-obs/flink/parquet/" + sinkTableName + "/";
        val fields = SchemaBuilder.record(DorisOneRow.class.getSimpleName()).fields();
        descInfo.forEach((columnName, mysqlType) -> {
            ReadableSchema readableSchema = formatType(columnName, mysqlType);
            formatInfo.put(columnName, readableSchema);
            fields.name(columnName)
                    .type(readableSchema.getSchema())
                    .withDefault(null);
        });
        schema = fields.endRecord();
        ArrayList<String> createTable = new ArrayList<>();
        createTable.add("DROP TABLE IF EXISTS test." + sinkTableName);
        createTable.add("CREATE EXTERNAL TABLE IF NOT EXISTS test." + sinkTableName + " (");
        formatInfo.forEach((k, v) -> createTable.add(String.format("  %s, %s", k, v.getType())));
        createTable.add(String.format(") STORED AS PARQUET location '%s';", path));
        log.info(createTable.stream().collect(Collectors.joining("\n", "\n", "\n")));
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

    private int computeMinBytesForDecimalPrecision(int precision) {
        int numBytes = 1;
        while (Math.pow(2.0, 8 * numBytes - 1) < Math.pow(10.0, precision)) {
            numBytes += 1;
        }
        return numBytes;
    }

    public void write(Map<String, Object> columnMap) {
        try {
            if (writer == null) {
                writer = getWriter();
            }
            GenericData.Record record = getRecord(columnMap);
            writer.write(record);
        } catch (Exception e) {
            String msg = "FsParquetWriter write error";
            log.error(msg, e);
            throw new RuntimeException(msg, e);
        }
    }

    private ParquetWriter<GenericRecord> getWriter() {
        try {
            return AvroParquetWriter.<GenericRecord>builder(new Path(path + UUID.randomUUID()))
                    .withSchema(schema)
                    .withRowGroupSize(PARQUET_ROW_GROUP_SIZE)
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

    public void flush() {
        try {
            writer.close();
            writer = null;
        } catch (Exception e) {
            String msg = "FsParquetWriter flush error";
            log.error(msg, e);
            throw new RuntimeException(msg, e);
        }
    }

    @Setter
    @Getter
    @Accessors(chain = true)
    private static abstract class ReadableSchema {
        protected Schema schema;

        protected abstract String getType();
    }

    private static final class StringSchema extends ReadableSchema {
        @Override
        protected String getType() {
            return "string";
        }
    }


    private static final class LongSchema extends ReadableSchema {
        @Override
        protected String getType() {
            return "bigint";
        }
    }

    @Getter
    @Setter
    @Accessors(chain = true)
    private static final class DecimalSchema extends ReadableSchema {
        private int precision;
        private int scale;

        @Override
        protected String getType() {
            return String.format("decimal(%d, %d)", precision, scale);
        }
    }
}
