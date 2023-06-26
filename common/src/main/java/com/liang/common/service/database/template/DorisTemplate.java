package com.liang.common.service.database.template;

import com.liang.common.dto.DorisOneRow;
import com.liang.common.dto.DorisSchema;
import com.liang.common.dto.config.DorisDbConfig;
import com.liang.common.service.AbstractCache;
import com.liang.common.service.Logging;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import lombok.Synchronized;
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
public class DorisTemplate extends AbstractCache<DorisSchema, DorisOneRow> {
    private final static int DEFAULT_CACHE_MILLISECONDS = 5000;
    private final static int DEFAULT_CACHE_RECORDS = 10240;
    private final HttpClientBuilder httpClientBuilder = HttpClients
            .custom()
            .setRedirectStrategy(new RedirectStrategy());
    private final Logging logging;
    private final List<String> fe;
    private final String auth;
    private final Random random = new Random();

    public DorisTemplate(String name) {
        super(DEFAULT_CACHE_MILLISECONDS, DEFAULT_CACHE_RECORDS, DorisOneRow::getSchema);
        DorisDbConfig dorisDbConfig = ConfigUtils.getConfig().getDorisDbConfigs().get(name);
        logging = new Logging(this.getClass().getSimpleName(), name);
        fe = dorisDbConfig.getFe();
        auth = basicAuthHeader(dorisDbConfig.getUser(), dorisDbConfig.getPassword());
    }

    @Override
    @Synchronized
    protected void updateImmediately(DorisSchema schema, List<DorisOneRow> dorisOneRows) {
        logging.beforeExecute();
        String uuid = UUID.randomUUID().toString();
        try (CloseableHttpClient client = httpClientBuilder.build()) {
            // url
            String target = fe.get(random.nextInt(fe.size()));
            String url = String.format("http://%s/api/%s/%s/_stream_load", target, schema.getDatabase(), schema.getTableName());
            // init put
            // common
            HttpPut put = new HttpPut(url);
            put.setHeader("label", uuid);
            put.setHeader(HttpHeaders.EXPECT, "100-continue");
            put.setHeader(HttpHeaders.AUTHORIZATION, auth);
            put.setHeader("format", "json");
            put.setHeader("strip_outer_array", "true");
            put.setHeader("exec_mem_limit", String.valueOf(100 * 1024 * 1024));
            // for unique table
            if (schema.getUniqueDeleteOn() != null || schema.getUniqueOrderBy() != null) {
                put.setHeader("merge_type", "MERGE");
                if (schema.getUniqueDeleteOn() != null) {
                    put.setHeader("delete", schema.getUniqueDeleteOn());
                }
                if (schema.getUniqueOrderBy() != null) {
                    put.setHeader("function_column.sequence_col", schema.getUniqueOrderBy());
                }
            }
            // for content
            List<Map<String, Object>> contentObject = dorisOneRows.parallelStream().map(DorisOneRow::getColumnMap).collect(Collectors.toList());
            List<String> keys = new ArrayList<>(contentObject.get(0).keySet());
            put.setHeader("columns", parseColumns(keys, schema.getDerivedColumns()));
            put.setHeader("jsonpaths", parseJsonPaths(keys));
            String contentString = JsonUtils.toString(contentObject);
            put.setEntity(new StringEntity(contentString, StandardCharsets.UTF_8));
            // execute
            try (CloseableHttpResponse response = client.execute(put)) {
                HttpEntity httpEntity = response.getEntity();
                String loadResult = EntityUtils.toString(httpEntity);
                int statusCode = response.getStatusLine().getStatusCode();
                if (statusCode == 200 && loadResult.contains("OK") && loadResult.contains("Success")) {
                    logging.afterExecute("stream load", uuid);
                } else {
                    throw new Exception(String.format("statusCode: %s, loadResult: %s", statusCode, loadResult));
                }
            }
        } catch (Exception e) {
            logging.ifError("stream load", uuid, e);
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

    @Slf4j
    private static class RedirectStrategy extends DefaultRedirectStrategy {
        @Override
        protected boolean isRedirectable(String method) {
            return true;
        }
    }
}
