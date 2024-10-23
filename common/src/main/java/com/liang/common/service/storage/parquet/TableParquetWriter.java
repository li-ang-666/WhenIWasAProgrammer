package com.liang.common.service.storage.parquet;

import cn.hutool.core.util.StrUtil;
import com.liang.common.service.storage.parquet.schema.ReadableSchema;
import com.liang.common.util.SqlUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class TableParquetWriter {
    private static final int PARQUET_ROW_GROUP_SIZE = 128 * 1024 * 1024;
    private final String path;
    private final Map<String, ReadableSchema> columnToSchema = new LinkedHashMap<>();
    private final Schema recordSchema;
    private ParquetWriter<GenericRecord> writer;

    public TableParquetWriter(String sinkTableName, List<ReadableSchema> columnSchemas) {
        path = "obs://hadoop-obs/flink/parquet/" + sinkTableName + "/";
        SchemaBuilder.FieldAssembler<Schema> recordSchemaBuilder = SchemaBuilder.record(sinkTableName).fields();
        columnSchemas.forEach(readableSchema -> {
            String columnName = readableSchema.getName();
            columnToSchema.put(columnName, readableSchema);
            recordSchemaBuilder.name(columnName)
                    .type(readableSchema.getSchema())
                    .withDefault(null);
        });
        recordSchema = recordSchemaBuilder.endRecord();
        // print create table
        ArrayList<String> createTable = new ArrayList<String>() {{
            add("DROP TABLE IF EXISTS test." + sinkTableName + ";");
            add("CREATE EXTERNAL TABLE IF NOT EXISTS test." + sinkTableName + " (");
            int maxLength = columnToSchema.keySet().stream().mapToInt(String::length).max()
                    .orElseThrow(() -> new RuntimeException("columns can not be empty"))
                    + 7;
            columnToSchema.forEach((k, v) -> {
                String formattedColumnName = SqlUtils.formatField(k);
                String formattedColumnType = v.getSqlType();
                add(String.format("  %s%s%s,",
                        formattedColumnName, StrUtil.repeat(" ", maxLength - formattedColumnName.length()), formattedColumnType));
            });
            add(String.format(") STORED AS PARQUET LOCATION '%s'", path));
        }};
        String logs = createTable.stream().collect(Collectors.joining("\n", "\n", ";"));
        log.info(StrUtil.replaceLast(logs, ",", " "));
    }

    public synchronized void write(Map<String, Object> columnMap) {
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

    private synchronized ParquetWriter<GenericRecord> getWriter() {
        try {
            return AvroParquetWriter.<GenericRecord>builder(new Path(path + UUID.randomUUID()))
                    .withSchema(recordSchema)
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

    private synchronized GenericData.Record getRecord(Map<String, Object> columnMap) {
        GenericRecordBuilder recordBuilder = new GenericRecordBuilder(recordSchema);
        columnMap.forEach((column, value) -> {
            if (columnToSchema.containsKey(column)) {
                recordBuilder.set(column, columnToSchema.get(column).formatValue(value));
            }
        });
        return recordBuilder.build();
    }

    public synchronized void flush() {
        try {
            if (writer != null) {
                writer.close();
                writer = null;
            }
        } catch (Exception e) {
            String msg = "FsParquetWriter flush error";
            log.error(msg, e);
            throw new RuntimeException(msg, e);
        }
    }
}
