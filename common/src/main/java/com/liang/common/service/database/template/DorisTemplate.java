package com.liang.common.service.database.template;

import com.liang.common.dto.DorisOneRow;
import com.liang.common.dto.DorisSchema;
import com.liang.common.dto.config.DorisConfig;
import com.liang.common.service.AbstractCache;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.DateUtils;
import com.liang.common.util.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

import static org.apache.http.HttpHeaders.AUTHORIZATION;
import static org.apache.http.HttpHeaders.EXPECT;

@Slf4j
public class DorisTemplate extends AbstractCache<DorisSchema, DorisOneRow> {
    private static final int DEFAULT_CACHE_MILLISECONDS = 1000 * 60;
    private static final int DEFAULT_CACHE_RECORDS = 10240;
    private static final int MAX_TRY_TIMES = 3;
    private static final int MAX_BYTE_BUFFER_SIZE = (int) (1.7 * 1024 * 1024 * 1024);
    private static final byte[] JSON_PREFIX = "[".getBytes(StandardCharsets.UTF_8);
    private static final byte[] JSON_SEPARATOR = ",".getBytes(StandardCharsets.UTF_8);
    private static final byte[] JSON_SUFFIX = "]".getBytes(StandardCharsets.UTF_8);
    private final HttpClientBuilder httpClientBuilder = HttpClients
            .custom()
            .setRedirectStrategy(new DorisRedirectStrategy());
    private final AtomicInteger fePointer = new AtomicInteger(0);
    private final List<String> fe;
    private final String auth;
    private DorisSchema schema;
    private List<String> keys;
    // self cache
    private ByteBuffer buffer;
    private int currentByteBufferSize = 0;
    private int currentRows = 0;
    private int maxRowSize = Integer.MIN_VALUE;

    public DorisTemplate(String name) {
        super(DEFAULT_CACHE_MILLISECONDS, DEFAULT_CACHE_RECORDS, DorisOneRow::getSchema);
        DorisConfig dorisConfig = ConfigUtils.getConfig().getDorisConfigs().get(name);
        fe = dorisConfig.getFe();
        auth = basicAuthHeader(dorisConfig.getUser(), dorisConfig.getPassword());
    }

    private String basicAuthHeader(String username, String password) {
        String tobeEncode = username + ":" + password;
        byte[] encoded = Base64.encodeBase64(tobeEncode.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encoded);
    }

    public void enableOffline() {
        if (buffer == null) {
            buffer = ByteBuffer.allocate(MAX_BYTE_BUFFER_SIZE);
        }
    }

    public void updateOffline(DorisOneRow dorisOneRow) {
        requireBufferNull(false);
        Map<String, Object> columnMap = dorisOneRow.getColumnMap();
        // the first row
        if (keys == null) {
            keys = new ArrayList<>(columnMap.keySet());
        }
        if (schema == null) {
            schema = dorisOneRow.getSchema();
        }
        // add prefix
        if (currentByteBufferSize == 0) {
            buffer.put(JSON_PREFIX);
            currentByteBufferSize += JSON_PREFIX.length;
        }
        // add separator
        if (currentRows > 0) {
            buffer.put(JSON_SEPARATOR);
            currentByteBufferSize += JSON_SEPARATOR.length;
        }
        // add content
        byte[] content = JsonUtils.toString(columnMap).getBytes(StandardCharsets.UTF_8);
        buffer.put(content);
        currentByteBufferSize += content.length;
        currentRows++;
        maxRowSize = Math.max(maxRowSize, content.length);
        // compare
        if (MAX_BYTE_BUFFER_SIZE - currentByteBufferSize < 1024 * maxRowSize) {
            flushOffline();
        }
    }

    public void flushOffline() {
        requireBufferNull(false);
        if (currentRows > 0) {
            // add suffix
            buffer.put(JSON_SUFFIX);
            currentByteBufferSize += JSON_SUFFIX.length;
            // execute
            HttpPut put = getCommonHttpPut();
            put.setEntity(new ByteArrayEntity(buffer.array(), 0, currentByteBufferSize));
            executePut(put);
        }
        buffer.clear();
        currentByteBufferSize = 0;
        currentRows = 0;
    }

    @Override
    protected void updateImmediately(DorisSchema schema, Collection<DorisOneRow> dorisOneRows) {
        requireBufferNull(true);
        assert buffer == null;
        this.schema = schema;
        List<Map<String, Object>> columnMaps = dorisOneRows.parallelStream().map(DorisOneRow::getColumnMap).collect(Collectors.toList());
        this.keys = new ArrayList<>(columnMaps.get(0).keySet());
        HttpPut put = getCommonHttpPut();
        put.setEntity(new StringEntity(JsonUtils.toString(columnMaps), StandardCharsets.UTF_8));
        executePut(put);
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
        // for unique delete
        if (schema.getUniqueDeleteOn() != null) {
            put.setHeader("merge_type", "MERGE");
            put.setHeader("delete", schema.getUniqueDeleteOn());
        }
        // column mapping
        if (schema.getDerivedColumns() != null && !schema.getDerivedColumns().isEmpty()) {
            put.setHeader("columns", parseColumns());
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
                DateUtils.fromUnixTime(System.currentTimeMillis() / 1000, "yyyyMMddHHmmss"), uuid);
    }

    private void requireBufferNull(boolean requireNull) {
        if (requireNull && buffer != null) {
            throw new RuntimeException("buffer should be null");
        } else if (!requireNull && buffer == null) {
            throw new RuntimeException("buffer should not be null");
        }
    }

    @Slf4j
    private static class DorisRedirectStrategy extends DefaultRedirectStrategy {
        @Override
        protected boolean isRedirectable(String method) {
            return true;
        }
    }
}
