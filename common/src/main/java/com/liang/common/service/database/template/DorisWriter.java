package com.liang.common.service.database.template;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import com.liang.common.dto.DorisOneRow;
import com.liang.common.dto.DorisSchema;
import com.liang.common.dto.config.DorisConfig;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

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
public class DorisWriter {
    private static final int MAX_TRY_TIMES = 3;
    private static final byte JSON_PREFIX = (byte) '[';
    private static final byte JSON_SEPARATOR = (byte) ',';
    private static final byte JSON_SUFFIX = (byte) ']';
    private final HttpClientBuilder httpClientBuilder = HttpClients
            .custom()
            .setRedirectStrategy(new DorisRedirectStrategy());
    private final AtomicInteger fePointer = new AtomicInteger(0);
    private final int maxBufferSize;
    private final ByteBuffer buffer;
    private final List<String> fe;
    private final String auth;
    private DorisSchema schema;
    private List<String> keys;
    private int currentBufferSize = 0;

    public DorisWriter(String name, int bufferSize) {
        maxBufferSize = bufferSize;
        buffer = ByteBuffer.allocate(bufferSize);
        DorisConfig dorisConfig = ConfigUtils.getConfig().getDorisConfigs().get(name);
        fe = dorisConfig.getFe();
        auth = basicAuthHeader(dorisConfig.getUser(), dorisConfig.getPassword());
    }

    public void write(DorisOneRow dorisOneRow) {
        synchronized (buffer) {
            Map<String, Object> columnMap = dorisOneRow.getColumnMap();
            // the first row
            if (schema == null) {
                schema = dorisOneRow.getSchema();
                keys = new ArrayList<>(columnMap.keySet());
            }
            byte[] content = JsonUtils.toString(columnMap).getBytes(StandardCharsets.UTF_8);
            // reserve 1 byte for suffix
            if (currentBufferSize + 1 + content.length >= maxBufferSize) flush();
            buffer.put(JSON_SEPARATOR);
            currentBufferSize += 1;
            buffer.put(content);
            currentBufferSize += content.length;
        }
    }

    public void flush() {
        synchronized (buffer) {
            if (currentBufferSize > 0) {
                buffer.put(0, JSON_PREFIX);
                buffer.put(JSON_SUFFIX);
                HttpPut put = getCommonHttpPut();
                put.setEntity(new ByteArrayEntity(buffer.array(), 0, currentBufferSize + 1));
                executePut(put);
            }
            buffer.clear();
            currentBufferSize = 0;
        }
    }

    private HttpPut getCommonHttpPut() {
        // common
        HttpPut put = new HttpPut();
        put.setHeader(EXPECT, "100-continue");
        put.setHeader(AUTHORIZATION, auth);
        put.setHeader("format", "json");
        put.setHeader("strip_outer_array", "true");
        put.setHeader("fuzzy_parse", "true");
        put.setHeader("num_as_string", "true");
        // columns
        if (CollUtil.isNotEmpty(schema.getDerivedColumns())) {
            put.setHeader("columns", parseColumns());
        }
        // unique delete
        if (StrUtil.isNotBlank(schema.getUniqueDeleteOn())) {
            put.setHeader("merge_type", "MERGE");
            put.setHeader("delete", schema.getUniqueDeleteOn());
            // delete must contains columns or hidden_columns
            if (!put.containsHeader("columns")) {
                put.setHeader("hidden_columns", DorisSchema.DEFAULT_UNIQUE_DELETE_COLUMN);
            }
        }
        // where
        if (StrUtil.isNotBlank(schema.getWhere())) {
            put.setHeader("where", schema.getWhere());
        }
        return put;
    }

    private String parseColumns() {
        List<String> columns = keys.parallelStream()
                .map(e -> "`" + e + "`")
                .collect(Collectors.toList());
        columns.addAll(schema.getDerivedColumns());
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
        return URI.create(String.format("http://%s/api/%s/%s/_stream_load", targetFe, schema.getDatabase(), schema.getTableName()));
    }

    private String getLabel() {
        String uuid = UUID.randomUUID().toString().replaceAll("-", "");
        return String.format("%s_%s_%s_%s", schema.getDatabase(), schema.getTableName(),
                LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss")), uuid);
    }

    private String basicAuthHeader(String username, String password) {
        String tobeEncode = username + ":" + password;
        byte[] encoded = Base64.encodeBase64(tobeEncode.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encoded);
    }

    @Slf4j
    private static class DorisRedirectStrategy extends DefaultRedirectStrategy {
        @Override
        protected boolean isRedirectable(String method) {
            return true;
        }
    }
}
