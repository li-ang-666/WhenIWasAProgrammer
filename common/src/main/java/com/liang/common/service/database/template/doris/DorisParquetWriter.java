package com.liang.common.service.database.template.doris;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import com.liang.common.dto.DorisOneRow;
import com.liang.common.dto.DorisSchema;
import com.liang.common.util.ConfigUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class DorisParquetWriter {
    private static final int PARQUET_MAGIC_NUMBER = 4;
    private static final int PARQUET_ROW_GROUP_SIZE = 32 * 1024 * 1024;
    private static final int MAX_BUFFER_SIZE = (int) (1.1 * PARQUET_ROW_GROUP_SIZE);
    private final DorisHelper dorisHelper;
    private final ByteBuffer buffer = ByteBuffer.allocate(MAX_BUFFER_SIZE);
    // parquet writer
    private Schema avroSchema;
    private ParquetWriter<GenericRecord> parquetWriter;
    // init when first row
    private DorisSchema dorisSchema;
    private List<String> keys;

    public DorisParquetWriter(String name) {
        dorisHelper = new DorisHelper(ConfigUtils.getConfig().getDorisConfigs().get(name));
    }

    @SneakyThrows(IOException.class)
    public void write(DorisOneRow dorisOneRow) {
        synchronized (buffer) {
            Map<String, Object> columnMap = dorisOneRow.getColumnMap();
            // the first row
            if (keys == null) {
                SchemaBuilder.FieldAssembler<Schema> schemaBuilder = SchemaBuilder.record(DorisOneRow.class.getSimpleName()).fields();
                columnMap.keySet().forEach(schemaBuilder::optionalString);
                avroSchema = schemaBuilder.endRecord();
                dorisSchema = dorisOneRow.getSchema();
                keys = new ArrayList<>(columnMap.keySet());
            }
            if (parquetWriter == null) {
                parquetWriter = AvroParquetWriter.<GenericRecord>builder(new OutputFileBuffer(buffer))
                        .withSchema(avroSchema)
                        .withRowGroupSize(PARQUET_ROW_GROUP_SIZE)
                        .build();
            }
            GenericRecord genericRecord = new GenericData.Record(avroSchema);
            columnMap.forEach((k, v) -> genericRecord.put(k, StrUtil.toStringOrNull(v)));
            parquetWriter.write(genericRecord);
            if (buffer.position() > PARQUET_MAGIC_NUMBER) flush();
        }
    }

    @SneakyThrows(IOException.class)
    public void flush() {
        synchronized (buffer) {
            if (buffer.position() > 0) {
                parquetWriter.close();
                parquetWriter = null;
                dorisHelper.execute(dorisSchema.getDatabase(), dorisSchema.getTableName(), this::setPut);
            }
            buffer.clear();
        }
    }

    private void setPut(HttpPut put) {
        // format
        put.setHeader("format", "parquet");
        // columns
        put.setHeader("columns", parseColumns());
        // unique delete
        if (StrUtil.isNotBlank(dorisSchema.getUniqueDeleteOn())) {
            put.setHeader("merge_type", "MERGE");
            put.setHeader("delete", dorisSchema.getUniqueDeleteOn());
        }
        // where
        if (StrUtil.isNotBlank(dorisSchema.getWhere())) {
            put.setHeader("where", dorisSchema.getWhere());
        }
        // entity
        put.setEntity(new ByteArrayEntity(buffer.array(), 0, buffer.position()));
    }

    private String parseColumns() {
        List<String> columns = keys.parallelStream()
                .map(e -> "`" + e + "`")
                .collect(Collectors.toList());
        if (CollUtil.isNotEmpty(dorisSchema.getDerivedColumns())) {
            columns.addAll(dorisSchema.getDerivedColumns());
        }
        return String.join(",", columns);
    }
}
