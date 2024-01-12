package com.liang.common.service.database.template;

import com.liang.common.dto.DorisOneRow;
import com.liang.common.dto.DorisSchema;
import com.liang.common.dto.config.DorisConfig;
import com.liang.common.service.AbstractCache;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.DateTimeUtils;
import com.liang.common.util.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

import static org.apache.http.HttpHeaders.AUTHORIZATION;
import static org.apache.http.HttpHeaders.EXPECT;

/**
 * use test_db;
 * -- SET show_hidden_columns=true;
 * -------------------------------------------------------
 * drop table if exists unique_test;
 * create table if not exists unique_test(
 * id int not null,
 * name text
 * )UNIQUE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 36;
 * -------------------------------------------------------
 * drop table if exists agg_test;
 * create table if not exists agg_test(
 * id int not null,
 * name text REPLACE_IF_NOT_NULL
 * )AGGREGATE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 36;
 */
@Slf4j
public class DorisTemplate extends AbstractCache<DorisSchema, DorisOneRow> {
    private final static int BUFFER_MAX_MB = 16; // 1kb/条 x 16000条
    private final static int DEFAULT_CACHE_MILLISECONDS = 5000;
    private final static int DEFAULT_CACHE_RECORDS = 10240;
    private final static int MAX_TRY_TIMES = 3;
    private final HttpClientBuilder httpClientBuilder = HttpClients
            .custom()
            .setRedirectStrategy(new DorisRedirectStrategy());
    private final List<String> fe;
    private final AtomicInteger fePointer = new AtomicInteger(0);
    private final String auth;

    public DorisTemplate(String name) {
        super(BUFFER_MAX_MB, DEFAULT_CACHE_MILLISECONDS, DEFAULT_CACHE_RECORDS, DorisOneRow::getSchema);
        DorisConfig dorisConfig = ConfigUtils.getConfig().getDorisConfigs().get(name);
        fe = dorisConfig.getFe();
        auth = basicAuthHeader(dorisConfig.getUser(), dorisConfig.getPassword());
    }

    @Override
    protected void updateImmediately(DorisSchema schema, Collection<DorisOneRow> dorisOneRows) {
        try (CloseableHttpClient client = httpClientBuilder.build()) {
            // init put
            HttpPut put = getHttpPut(schema, dorisOneRows);
            // execute put
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

    private String basicAuthHeader(String username, String password) {
        final String tobeEncode = username + ":" + password;
        byte[] encoded = Base64.encodeBase64(tobeEncode.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encoded);
    }

    private HttpPut getHttpPut(DorisSchema schema, Collection<DorisOneRow> dorisOneRows) {
        // common
        HttpPut put = new HttpPut();
        put.setHeader(EXPECT, "100-continue");
        put.setHeader(AUTHORIZATION, auth);
        put.setHeader("format", "json");
        put.setHeader("strip_outer_array", "true");
        put.setHeader("num_as_string", "true");
        put.setHeader("send_batch_parallelism", "1");
        put.setHeader("two_phase_commit", "false");
        // for unique delete
        if (schema.getUniqueDeleteOn() != null) {
            put.setHeader("merge_type", "MERGE");
            put.setHeader("delete", schema.getUniqueDeleteOn());
        }
        // content
        List<Map<String, Object>> columnMaps = dorisOneRows.parallelStream().map(DorisOneRow::getColumnMap).collect(Collectors.toList());
        List<String> keys = new ArrayList<>(columnMaps.get(0).keySet());
        put.setHeader("columns", parseColumns(keys, schema.getDerivedColumns()));
        put.setHeader("jsonpaths", parseJsonPaths(keys));
        put.setEntity(new StringEntity(JsonUtils.toString(columnMaps), StandardCharsets.UTF_8));
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

    private URI getUri(String database, String table) {
        String targetFe = fe.get(fePointer.getAndIncrement() % fe.size());
        return URI.create(String.format("http://%s/api/%s/%s/_stream_load", targetFe, database, table));
    }

    private String getLabel(String database, String table) {
        String uuid = UUID.randomUUID().toString().replaceAll("-", "");
        return String.format("%s_%s_%s_%s", database, table,
                DateTimeUtils.fromUnixTime(System.currentTimeMillis() / 1000, "yyyyMMddHHmmss"), uuid);
    }

    @Slf4j
    private static class DorisRedirectStrategy extends DefaultRedirectStrategy {
        @Override
        protected boolean isRedirectable(String method) {
            return true;
        }
    }
}
