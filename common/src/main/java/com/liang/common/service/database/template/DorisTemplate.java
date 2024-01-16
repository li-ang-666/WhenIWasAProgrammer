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
    private static final int DEFAULT_CACHE_MILLISECONDS = 30000;
    private static final int DEFAULT_CACHE_RECORDS = 10240;
    private static final int MAX_TRY_TIMES = 3;
    private static final int MAX_BYTE_BUFFER_SIZE = 512 * 1024 * 1024;
    private static final byte[] JSON_PREFIX = "[".getBytes(StandardCharsets.UTF_8);
    private static final byte[] JSON_SEPARATOR = ",".getBytes(StandardCharsets.UTF_8);
    private static final byte[] JSON_SUFFIX = "]".getBytes(StandardCharsets.UTF_8);
    private final HttpClientBuilder httpClientBuilder = HttpClients
            .custom()
            .setRedirectStrategy(new DorisRedirectStrategy());
    private final AtomicInteger fePointer = new AtomicInteger(0);
    private final List<String> fe;
    private final String auth;
    // self cache
    private final ByteBuffer buffer;
    private final DorisSchema schema;
    private final List<String> keys;
    private int currentByteBufferSize = 0;
    private int currentRows = 0;
    private int maxRowSize = 0;

    public DorisTemplate(String name) {
        super(DEFAULT_CACHE_MILLISECONDS, DEFAULT_CACHE_RECORDS, DorisOneRow::getSchema);
        DorisConfig dorisConfig = ConfigUtils.getConfig().getDorisConfigs().get(name);
        fe = dorisConfig.getFe();
        auth = basicAuthHeader(dorisConfig.getUser(), dorisConfig.getPassword());
        this.buffer = null;
        this.schema = null;
        this.keys = null;
    }

    public DorisTemplate(String name, DorisSchema schema) {
        super(DEFAULT_CACHE_MILLISECONDS, DEFAULT_CACHE_RECORDS, DorisOneRow::getSchema);
        DorisConfig dorisConfig = ConfigUtils.getConfig().getDorisConfigs().get(name);
        fe = dorisConfig.getFe();
        auth = basicAuthHeader(dorisConfig.getUser(), dorisConfig.getPassword());
        this.buffer = ByteBuffer.allocate(MAX_BYTE_BUFFER_SIZE);
        this.schema = schema;
        this.keys = new ArrayList<>();
    }

    @Override
    protected void updateImmediately(DorisSchema schema, Collection<DorisOneRow> dorisOneRows) {
        if (buffer != null) {
            throw new RuntimeException("due to the `Constructor`, maybe you need to use cacheBatch() and flushBatch()");
        }
        HttpPut put = getHttpPutWithStringEntity(schema, dorisOneRows.parallelStream().map(DorisOneRow::getColumnMap).collect(Collectors.toList()));
        executePut(put, schema);
    }

    public boolean cacheBatch(Map<String, Object> map) {
        Map<String, Object> columnMap = new TreeMap<>(map);
        if (buffer == null) {
            throw new RuntimeException("due to the `Constructor`, maybe you need to use update() and flush()");
        }
        // the first row
        if (keys.isEmpty()) keys.addAll(columnMap.keySet());
        // add prefix
        if (currentByteBufferSize == 0 && currentRows == 0) {
            buffer.put(JSON_PREFIX);
            currentByteBufferSize += JSON_PREFIX.length;
        }
        // add separator
        if (currentByteBufferSize > 0 && currentRows > 0) {
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
        return MAX_BYTE_BUFFER_SIZE - currentByteBufferSize > 1024 * maxRowSize;
    }

    public void flushBatch() {
        if (currentByteBufferSize == 0 && currentRows == 0) return;
        // add suffix
        buffer.put(JSON_SUFFIX);
        currentByteBufferSize += JSON_SUFFIX.length;
        // execute
        HttpPut put = getHttpPutWithBinaryEntity(schema);
        executePut(put, schema);
        buffer.clear();
        currentByteBufferSize = 0;
        currentRows = 0;
    }

    private String basicAuthHeader(String username, String password) {
        String tobeEncode = username + ":" + password;
        byte[] encoded = Base64.encodeBase64(tobeEncode.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encoded);
    }

    private HttpPut getHttpPutWithStringEntity(DorisSchema schema, List<Map<String, Object>> columnMaps) {
        HttpPut put = getCommonHttpPut(schema, new ArrayList<>(columnMaps.get(0).keySet()));
        put.setEntity(new StringEntity(JsonUtils.toString(columnMaps), StandardCharsets.UTF_8));
        return put;
    }

    private HttpPut getHttpPutWithBinaryEntity(DorisSchema schema) {
        HttpPut put = getCommonHttpPut(schema, keys);
        put.setEntity(new ByteArrayEntity(buffer.array(), 0, currentByteBufferSize));
        return put;
    }

    private HttpPut getCommonHttpPut(DorisSchema schema, List<String> keys) {
        // common
        HttpPut put = new HttpPut();
        put.setHeader(EXPECT, "100-continue");
        put.setHeader(AUTHORIZATION, auth);
        put.setHeader("format", "json");
        put.setHeader("num_as_string", "true");
        put.setHeader("strip_outer_array", "true");
        put.setHeader("fuzzy_parse", "true");
        // for unique delete
        if (schema.getUniqueDeleteOn() != null) {
            put.setHeader("merge_type", "MERGE");
            put.setHeader("delete", schema.getUniqueDeleteOn());
        }
        // column mapping
        put.setHeader("columns", parseColumns(keys, schema.getDerivedColumns()));
        put.setHeader("jsonpaths", parseJsonPaths(keys));
        return put;
    }

    private String parseColumns(List<String> keys, List<String> derivedColumns) {
        List<String> columns = keys.parallelStream()
                .map(e -> "`" + e + "`")
                .collect(Collectors.toList());
        if (derivedColumns != null && !derivedColumns.isEmpty()) {
            columns.addAll(derivedColumns);
        }
        return String.join(",", columns);
    }

    private String parseJsonPaths(List<String> keys) {
        return keys.parallelStream()
                .map(e -> "\"$." + e + "\"")
                .collect(Collectors.joining(",", "[", "]"));
    }

    private void executePut(HttpPut put, DorisSchema schema) {
        try (CloseableHttpClient client = httpClientBuilder.build()) {
            int tryTimes = MAX_TRY_TIMES;
            while (tryTimes-- > 0) {
                // 负载均衡 & label
                put.setURI(getUri(schema.getDatabase(), schema.getTableName()));
                put.setHeader("label", getLabel(schema.getDatabase(), schema.getTableName()));
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

    private URI getUri(String database, String table) {
        String targetFe = fe.get(fePointer.getAndIncrement() % fe.size());
        return URI.create(String.format("http://%s/api/%s/%s/_stream_load", targetFe, database, table));
    }

    private String getLabel(String database, String table) {
        String uuid = UUID.randomUUID().toString().replaceAll("-", "");
        return String.format("%s_%s_%s_%s", database, table,
                DateUtils.fromUnixTime(System.currentTimeMillis() / 1000, "yyyyMMddHHmmss"), uuid);
    }

    @Slf4j
    private static class DorisRedirectStrategy extends DefaultRedirectStrategy {
        @Override
        protected boolean isRedirectable(String method) {
            return true;
        }
    }
}
