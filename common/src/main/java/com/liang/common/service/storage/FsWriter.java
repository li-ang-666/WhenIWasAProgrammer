package com.liang.common.service.storage;

import cn.hutool.core.util.StrUtil;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

@Slf4j
public class FsWriter {
    private static final CompressionCodecName COMPRESSION_CODEC_NAME = CompressionCodecName.GZIP;
    private static final int ROW_GROUP_SIZE = 128 * 1024 * 1024;
    private static final String SUFFIX = ".parquet.gz";
    private final UUID uuid = UUID.randomUUID();
    private final String folder;
    private Schema schema;
    private ParquetWriter<GenericRecord> parquetWriter;

    public FsWriter(String folder) {
        this.folder = folder.replaceAll("/+$", "") + "/";
    }

    @SneakyThrows
    public synchronized void write(Map<String, Object> columnMap) {
        columnMap = new TreeMap<>(columnMap);
        if (schema == null) {
            SchemaBuilder.FieldAssembler<Schema> schemaBuilder = SchemaBuilder.record("ColumnMap").fields();
            columnMap.keySet().forEach(schemaBuilder::optionalString);
            schema = schemaBuilder.endRecord();
        }
        if (parquetWriter == null) {
            parquetWriter = newParquetWriter();
        }
        GenericRecordBuilder genericRecordBuilder = new GenericRecordBuilder(schema);
        columnMap.forEach((k, v) -> genericRecordBuilder.set(k, StrUtil.toStringOrNull(v)));
        parquetWriter.write(genericRecordBuilder.build());
    }

    @SneakyThrows
    public synchronized void flush() {
        if (parquetWriter == null) {
            return;
        }
        parquetWriter.close();
        parquetWriter = null;
    }

    @SneakyThrows
    private ParquetWriter<GenericRecord> newParquetWriter() {
        String now = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
        String file = String.format("%s%s.%s%s", folder, uuid, now, SUFFIX);
        Path path = new Path(file);
        return AvroParquetWriter.<GenericRecord>builder(path)
                .withCompressionCodec(COMPRESSION_CODEC_NAME)
                .withSchema(schema)
                .withRowGroupSize(ROW_GROUP_SIZE)
                .build();
    }
}
