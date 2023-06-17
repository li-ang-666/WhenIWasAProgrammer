package com.liang.common.service.database.template;

import com.liang.common.dto.DorisOneRow;
import com.liang.common.dto.DorisSchema;
import com.liang.common.dto.config.DorisDbConfig;
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
 * <p>drop table if exists stream_load_test;
 * <p>create table if not exists stream_load_test(
 * <p>     id int,
 * <p>     name text
 * <p>)
 * <p>UNIQUE KEY(`id`)
 * <p>DISTRIBUTED BY HASH(`id`) BUCKETS 1
 * <p>PROPERTIES (
 * <p>     "function_column.sequence_type" = 'largeint',
 * <p>     "replication_num" = "1",
 * <p>     "in_memory" = "false"
 * <p>);
 * <p>SET show_hidden_columns=true;
 */
@Slf4j
public class DorisTemplate {
    private final HttpClientBuilder httpClientBuilder = HttpClients
            .custom()
            .setRedirectStrategy(new DefaultRedirectStrategy() {
                @Override
                protected boolean isRedirectable(String method) {
                    return true;
                }
            });
    private final TemplateLogger logger;
    private final List<String> fe;
    private final String auth;
    private final Random random = new Random();
    private final Map<DorisSchema, List<DorisOneRow>> cache = new HashMap<>();

    public DorisTemplate(String name) {
        logger = new TemplateLogger(this.getClass().getSimpleName(), name);
        DorisDbConfig dorisDbConfig = ConfigUtils.getConfig().getDorisDbConfigs().get(name);
        fe = dorisDbConfig.getFe();
        auth = basicAuthHeader(dorisDbConfig.getUser(), dorisDbConfig.getPassword());
        new Thread(new Sender(this)).start();
    }

    public void load(DorisOneRow... dorisOneRows) {
        load(Arrays.asList(dorisOneRows));
    }

    public void load(List<DorisOneRow> dorisOneRows) {
        for (DorisOneRow dorisOneRow : dorisOneRows) {
            synchronized (cache) {
                DorisSchema key = dorisOneRow.getSchema();
                cache.putIfAbsent(key, new ArrayList<>());
                cache.get(key).add(dorisOneRow);
            }
        }
    }

    private void load(DorisSchema schema, List<DorisOneRow> rows) {
        logger.beforeExecute("load", rows);
        try (CloseableHttpClient client = httpClientBuilder.build()) {
            String target = fe.get(random.nextInt(fe.size()));
            String url = String.format("http://%s/api/%s/%s/_stream_load", target, schema.getDatabase(), schema.getTableName());
            HttpPut put = new HttpPut(url);
            put.setHeader(HttpHeaders.EXPECT, "100-continue");
            put.setHeader(HttpHeaders.AUTHORIZATION, auth);
            List<Map<String, Object>> contents = rows.parallelStream().map(DorisOneRow::getColumnMap).collect(Collectors.toList());
            put.setEntity(new StringEntity(JsonUtils.toString(contents), StandardCharsets.UTF_8));
            put.setHeader("format", "json");
            put.setHeader("strip_outer_array", "true");
            put.setHeader("merge_type", "MERGE");
            if (schema.getUniqueDeleteOn() != null) {
                put.setHeader("delete", schema.getUniqueDeleteOn());
            }
            if (schema.getUniqueOrderBy() != null) {
                put.setHeader("function_column.sequence_col", schema.getUniqueOrderBy());
            }
            List<String> keys = new ArrayList<>(contents.get(0).keySet());
            put.setHeader("columns", parseColumns(keys, schema.getDerivedColumns()));
            put.setHeader("jsonpaths", parseJsonPaths(keys));
            //执行 put
            try (CloseableHttpResponse response = client.execute(put)) {
                HttpEntity httpEntity = response.getEntity();
                String loadResult = EntityUtils.toString(httpEntity);
                int statusCode = response.getStatusLine().getStatusCode();
                if (statusCode == 200 && loadResult.contains("OK") && loadResult.contains("Success")) {
                    logger.afterExecute("load", rows);
                } else {
                    throw new Exception(String.format("stream load error, statusCode: %s, loadResult: %s", statusCode, loadResult));
                }
            }
        } catch (Exception e) {
            logger.ifError("load", rows, e);
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
        if (derivedColumns != null && derivedColumns.size() > 0) {
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

        public Sender(DorisTemplate dorisTemplate) {
            this.dorisTemplate = dorisTemplate;
        }

        @Override
        @SneakyThrows
        public void run() {
            while (true) {
                TimeUnit.MILLISECONDS.sleep(1000);
                Map<DorisSchema, List<DorisOneRow>> copyCache;
                synchronized (dorisTemplate.cache) {
                    copyCache = new HashMap<>(dorisTemplate.cache);
                    dorisTemplate.cache.clear();
                }
                for (Map.Entry<DorisSchema, List<DorisOneRow>> entry : copyCache.entrySet()) {
                    dorisTemplate.load(entry.getKey(), entry.getValue());
                }
            }
        }
    }
}
