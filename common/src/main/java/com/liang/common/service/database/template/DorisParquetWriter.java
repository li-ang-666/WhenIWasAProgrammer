package com.liang.common.service.database.template;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import com.liang.common.dto.DorisOneRow;
import com.liang.common.dto.DorisSchema;
import com.liang.common.dto.config.DorisConfig;
import com.liang.common.util.ConfigUtils;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

import static org.apache.http.HttpHeaders.AUTHORIZATION;
import static org.apache.http.HttpHeaders.EXPECT;

@Slf4j
public class DorisParquetWriter {
    private static final int MAX_TRY_TIMES = 3;
    private static final int PARQUET_MAGIC_NUMBER = 4;
    private static final int PARQUET_ROW_GROUP_SIZE = 32 * 1024 * 1024;
    private static final int MAX_BUFFER_SIZE = (int) (1.5 * PARQUET_ROW_GROUP_SIZE);
    private final HttpClientBuilder httpClientBuilder = HttpClients
            .custom()
            .setRedirectStrategy(new DorisRedirectStrategy());
    private final AtomicInteger fePointer = new AtomicInteger(0);
    private final ByteBuffer buffer = ByteBuffer.allocate(MAX_BUFFER_SIZE);
    private final List<String> fe;
    private final String auth;
    private Schema avroSchema;
    private DorisSchema dorisSchema;
    private List<String> keys;
    private ParquetWriter<GenericRecord> parquetWriter;

    public DorisParquetWriter(String name) {
        DorisConfig dorisConfig = ConfigUtils.getConfig().getDorisConfigs().get(name);
        fe = dorisConfig.getFe();
        auth = basicAuthHeader(dorisConfig.getUser(), dorisConfig.getPassword());
    }

    @SneakyThrows(IOException.class)
    public void write(DorisOneRow dorisOneRow) {
        synchronized (buffer) {
            Map<String, Object> columnMap = dorisOneRow.getColumnMap();
            // the first row
            if (avroSchema == null) {
                SchemaBuilder.FieldAssembler<Schema> schemaBuilder = SchemaBuilder.record("DorisOneRow").fields();
                columnMap.keySet().forEach(key -> schemaBuilder.name(key).type().nullable().stringType().noDefault());
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
                HttpPut put = getCommonHttpPut();
                put.setEntity(new ByteArrayEntity(buffer.array(), 0, buffer.position()));
                executePut(put);
            }
            buffer.clear();
            parquetWriter = null;
        }
    }

    private HttpPut getCommonHttpPut() {
        // common
        HttpPut put = new HttpPut();
        put.setHeader(EXPECT, "100-continue");
        put.setHeader(AUTHORIZATION, auth);
        put.setHeader("format", "parquet");
        // unique delete
        if (StrUtil.isNotBlank(dorisSchema.getUniqueDeleteOn())) {
            put.setHeader("merge_type", "MERGE");
            put.setHeader("delete", dorisSchema.getUniqueDeleteOn());
        }
        // columns
        put.setHeader("columns", parseColumns());
        // where
        if (StrUtil.isNotBlank(dorisSchema.getWhere())) {
            put.setHeader("where", dorisSchema.getWhere());
        }
        return put;
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

    private void executePut(HttpPut put) {
        try (CloseableHttpClient client = httpClientBuilder.build()) {
            int tryTimes = MAX_TRY_TIMES;
            while (tryTimes-- > 0) {
                // 负载均衡 & label
                put.setURI(getUri());
                put.setHeader("label", getLabel());
                try (CloseableHttpResponse response = client.execute(put)) {
                    int statusCode = response.getStatusLine().getStatusCode();
                    String loadResult = EntityUtils.toString(response.getEntity());
                    if (statusCode == 200 && loadResult.contains("Success") && loadResult.contains("OK")) { // Status = Success, Message = OK
                        log.info("stream load success, loadResult:\n{}", loadResult);
                        tryTimes = 0;
                    } else if (statusCode == 200 && loadResult.contains("Publish Timeout") && loadResult.contains("PUBLISH_TIMEOUT")) { // Status = Publish Timeout, Message = PUBLISH_TIMEOUT
                        log.warn("stream load success, loadResult:\n{}", loadResult);
                        tryTimes = 0;
                    } else if (tryTimes == 0) {
                        log.error("stream load failed for {} times, statusCode: {}, loadResult:\n{}", MAX_TRY_TIMES, statusCode, loadResult);
                    } else {
                        LockSupport.parkUntil(System.currentTimeMillis() + 1000);
                    }
                }
            }
        } catch (Exception e) {
            log.error("stream load failed without loadResult", e);
        }
    }

    private URI getUri() {
        String targetFe = fe.get(fePointer.getAndIncrement() % fe.size());
        return URI.create(String.format("http://%s/api/%s/%s/_stream_load", targetFe, dorisSchema.getDatabase(), dorisSchema.getTableName()));
    }

    private String getLabel() {
        String uuid = UUID.randomUUID().toString().replaceAll("-", "");
        return String.format("%s_%s_%s_%s", dorisSchema.getDatabase(), dorisSchema.getTableName(),
                LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss")), uuid);
    }

    private String basicAuthHeader(String username, String password) {
        String tobeEncode = username + ":" + password;
        byte[] encoded = Base64.encodeBase64(tobeEncode.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encoded);
    }

    @Slf4j
    private static final class DorisRedirectStrategy extends DefaultRedirectStrategy {
        @Override
        protected boolean isRedirectable(String method) {
            return true;
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
