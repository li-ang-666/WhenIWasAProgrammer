package com.liang.common.service.database.template;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

@Slf4j
public class MemoryParquetWriter {
    private static final int PARQUET_ROW_GROUP_SIZE = 32 * 1024 * 1024;
    private static final int PARQUET_PAGE_SIZE = 4 * 1024 * 1024;
    private static final int PARQUET_MAGIC_NUMBER = 4;
    private static final int BUFFER_SIZE = (int) (1.5 * PARQUET_ROW_GROUP_SIZE);
    private final ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
    private Schema avroSchema;
    private ParquetWriter<GenericRecord> parquetWriter;

    @SneakyThrows(IOException.class)
    public void write(Map<String, Object> columnMap) {
        synchronized (buffer) {
            // the first row
            if (avroSchema == null) {
                SchemaBuilder.FieldAssembler<Schema> schemaBuilder = SchemaBuilder.record("DorisOneRow").fields();
                columnMap.keySet().forEach(key -> schemaBuilder.name(key).type().nullable().stringType().noDefault());
                avroSchema = schemaBuilder.endRecord();
            }
            if (parquetWriter == null) {
                parquetWriter = AvroParquetWriter.<GenericRecord>builder(new OutputFileBuffer(buffer))
                        .withSchema(avroSchema)
                        .withRowGroupSize(PARQUET_ROW_GROUP_SIZE)
                        .withPageSize(PARQUET_PAGE_SIZE)
                        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                        .build();
            }
            GenericRecord genericRecord = new GenericData.Record(avroSchema);
            columnMap.forEach((k, v) -> genericRecord.put(k, v != null ? String.valueOf(v) : null));
            parquetWriter.write(genericRecord);
            if (buffer.position() > PARQUET_MAGIC_NUMBER) flush();
        }
    }

    @SneakyThrows(IOException.class)
    public void flush() {
        synchronized (buffer) {
            if (buffer.position() > 0) {
                parquetWriter.close();
            }
            buffer.clear();
            parquetWriter = null;
        }
    }

    @RequiredArgsConstructor
    private static final class OutputFileBuffer implements OutputFile {
        private final ByteBuffer byteBuffer;

        @Override
        public PositionOutputStream create(long blockSizeHint) {
            return new PositionOutputStreamBuffer(byteBuffer);
        }

        @Override
        public PositionOutputStream createOrOverwrite(long blockSizeHint) {
            return new PositionOutputStreamBuffer(byteBuffer);
        }

        @Override
        public boolean supportsBlockSize() {
            return false;
        }

        @Override
        public long defaultBlockSize() {
            return 0;
        }

        @RequiredArgsConstructor
        private final static class PositionOutputStreamBuffer extends PositionOutputStream {
            private final ByteBuffer byteBuffer;

            @Override
            public long getPos() {
                return byteBuffer.position();
            }

            @Override
            public void write(int b) {
                byteBuffer.put((byte) b);
            }
        }
    }
}
