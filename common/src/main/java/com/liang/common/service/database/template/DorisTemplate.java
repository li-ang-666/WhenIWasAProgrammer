package com.liang.common.service.database.template;

import com.liang.common.dto.DorisOneRow;
import com.liang.common.dto.DorisSchema;
import com.liang.common.dto.config.DorisDbConfig;
import com.liang.common.service.Logging;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * drop table if exists stream_load_test;
 * create table if not exists stream_load_test(id int,name text)
 * UNIQUE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
 * PROPERTIES (
 * "function_column.sequence_type" = 'largeint',
 * "replication_num" = "1",
 * "in_memory" = "false"
 * );
 * -------------------------------------------------------
 * drop table if exists agg_test;
 * create table if not exists agg_test(id int,name text REPLACE_IF_NOT_NULL)
 * aggregate KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
 * PROPERTIES (
 * "replication_num" = "1",
 * "in_memory" = "false"
 * );
 * --------------------------------------------------------
 * SET show_hidden_columns=true;
 */
@Slf4j
public class DorisTemplate {
    private final static int DEFAULT_CACHE_TIME = 5000;
    private final static int DEFAULT_CACHE_SIZE = 10240;
    private final HttpClientBuilder httpClientBuilder = HttpClients
            .custom()
            .setRedirectStrategy(new RedirectStrategy());
    private final Logging logger;
    private final List<String> fe;
    private final String auth;
    private final Random random = new Random();
    private final Map<DorisSchema, List<DorisOneRow>> cache = new HashMap<>();

    private volatile boolean enableCache = false;

    public DorisTemplate(String name) {
        DorisDbConfig dorisDbConfig = ConfigUtils.getConfig().getDorisDbConfigs().get(name);
        logger = new Logging(this.getClass().getSimpleName(), name);
        fe = dorisDbConfig.getFe();
        auth = basicAuthHeader(dorisDbConfig.getUser(), dorisDbConfig.getPassword());
    }

    public DorisTemplate enableCache() {
        return enableCache(DEFAULT_CACHE_TIME);
    }

    public DorisTemplate enableCache(int cacheTime) {
        if (!enableCache) {
            enableCache = true;
            new Thread(new Sender(this, cacheTime)).start();
        }
        return this;
    }

    public void load(DorisOneRow... dorisOneRows) {
        if (dorisOneRows == null || dorisOneRows.length == 0) {
            return;
        }
        load(Arrays.asList(dorisOneRows));
    }

    public void load(List<DorisOneRow> dorisOneRows) {
        if (dorisOneRows == null || dorisOneRows.isEmpty()) {
            return;
        }
        for (DorisOneRow dorisOneRow : dorisOneRows) {
            synchronized (cache) {
                DorisSchema key = dorisOneRow.getSchema();
                cache.putIfAbsent(key, new ArrayList<>());
                List<DorisOneRow> list = cache.get(key);
                list.add(dorisOneRow);
                if (list.size() >= DEFAULT_CACHE_SIZE) {
                    load(key, list);
                    cache.remove(key);
                }
            }
        }
        if (!enableCache) {
            for (Map.Entry<DorisSchema, List<DorisOneRow>> entry : cache.entrySet()) {
                load(entry.getKey(), entry.getValue());
            }
            cache.clear();
        }
    }

    private synchronized void load(DorisSchema schema, List<DorisOneRow> dorisOneRows) {
        if (dorisOneRows == null || dorisOneRows.isEmpty()) {
            return;
        }
        logger.beforeExecute();
        try (CloseableHttpClient client = httpClientBuilder.build()) {
            String target = fe.get(random.nextInt(fe.size()));
            String url = String.format("http://%s/api/%s/%s/_stream_load", target, schema.getDatabase(), schema.getTableName());
            HttpPut put = new HttpPut(url);
            put.setHeader(HttpHeaders.EXPECT, "100-continue");
            put.setHeader(HttpHeaders.AUTHORIZATION, auth);
            List<Map<String, Object>> contents = dorisOneRows.parallelStream().map(DorisOneRow::getColumnMap).collect(Collectors.toList());
            put.setEntity(new StringEntity(JsonUtils.toString(contents), StandardCharsets.UTF_8));
            put.setHeader("format", "json");
            put.setHeader("strip_outer_array", "true");
            String mergeType = (schema.getUniqueDeleteOn() != null || schema.getUniqueOrderBy() != null) ? "MERGE" : "APPEND";
            put.setHeader("merge_type", mergeType);
            if (schema.getUniqueDeleteOn() != null) {
                put.setHeader("delete", schema.getUniqueDeleteOn());
            }
            if (schema.getUniqueOrderBy() != null) {
                put.setHeader("function_column.sequence_col", schema.getUniqueOrderBy());
            }
            List<String> keys = new ArrayList<>(contents.get(0).keySet());
            put.setHeader("columns", parseColumns(keys, schema.getDerivedColumns()));
            put.setHeader("jsonpaths", parseJsonPaths(keys));
            try (CloseableHttpResponse response = client.execute(put)) {
                HttpEntity httpEntity = response.getEntity();
                String loadResult = EntityUtils.toString(httpEntity);
                int statusCode = response.getStatusLine().getStatusCode();
                if (statusCode == 200 && loadResult.contains("OK") && loadResult.contains("Success")) {
                    logger.afterExecute("load", dorisOneRows);
                } else {
                    throw new Exception(String.format("stream load error, statusCode: %s, loadResult: %s", statusCode, loadResult));
                }
            }
        } catch (Exception e) {
            logger.ifError("load", dorisOneRows, e);
        }
    }

    private String basicAuthHeader(String username, String password) {
        final String tobeEncode = username + ":" + password;
        byte[] encoded = Base64.encodeBase64(tobeEncode.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encoded);
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

    private static class Sender implements Runnable {
        private final DorisTemplate dorisTemplate;
        private final int cacheTime;

        public Sender(DorisTemplate dorisTemplate, int cacheTime) {
            this.dorisTemplate = dorisTemplate;
            this.cacheTime = cacheTime;
        }

        @Override
        @SneakyThrows
        @SuppressWarnings("InfiniteLoopStatement")
        public void run() {
            while (true) {
                TimeUnit.MILLISECONDS.sleep(cacheTime);
                if (dorisTemplate.cache.isEmpty()) {
                    continue;
                }
                Map<DorisSchema, List<DorisOneRow>> copyCache;
                synchronized (dorisTemplate.cache) {
                    if (dorisTemplate.cache.isEmpty()) {
                        continue;
                    }
                    copyCache = new HashMap<>(dorisTemplate.cache);
                    dorisTemplate.cache.clear();
                }
                for (Map.Entry<DorisSchema, List<DorisOneRow>> entry : copyCache.entrySet()) {
                    dorisTemplate.load(entry.getKey(), entry.getValue());
                }
            }
        }
    }

    @Slf4j
    private static class RedirectStrategy extends DefaultRedirectStrategy {
        @Override
        protected boolean isRedirectable(String method) {
            return true;
        }
    }
}
