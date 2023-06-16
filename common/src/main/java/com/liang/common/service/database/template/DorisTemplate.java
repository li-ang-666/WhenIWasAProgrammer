package com.liang.common.service.database.template;

import com.liang.common.dto.config.DorisConfig;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
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
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

@Slf4j
public class DorisTemplate {
    private final Random random = new Random();
    private final HttpClientBuilder httpClientBuilder = HttpClients
            .custom()
            .setRedirectStrategy(new DefaultRedirectStrategy() {
                @Override
                protected boolean isRedirectable(String method) {
                    return true;
                }
            });
    private final String path;
    private final List<String> feHosts;

    public DorisTemplate(String name) {
        DorisConfig dorisConfig = ConfigUtils.getConfig().getDorisConfigs().get(name);
        path = String.format("http://%s:%s/api/%s/%s/_stream_load",
                "%s", dorisConfig.getPort(), dorisConfig.getDatabase(), "%s");
        feHosts = dorisConfig.getFeHttpHosts();
    }

    public void streamLoad(String tableName, List<Map<String, Object>> contents) {
        try (CloseableHttpClient client = httpClientBuilder.build()) {
            String host = feHosts.get(random.nextInt(feHosts.size()));
            String url = String.format(path, host, tableName);
            HttpPut put = new HttpPut(url);
            put.setHeader(HttpHeaders.EXPECT, "100-continue");
            put.setHeader(HttpHeaders.AUTHORIZATION, basicAuthHeader("dba", "Tyc@1234"));
            put.setEntity(new StringEntity(JsonUtils.toString(contents), StandardCharsets.UTF_8));
            put.setHeader("label", String.valueOf(System.currentTimeMillis()));
            put.setHeader("format", "json");
            put.setHeader("strip_outer_array", "true");
            put.setHeader("merge_type", "MERGE");
            put.setHeader("delete", "__DORIS_DELETE_SIGN__ = 1");
            put.setHeader("function_column.sequence_col", "__DORIS_SEQUENCE_COL__");
            put.setHeader("columns", parseColumns(contents));
            put.setHeader("jsonpaths", parseJsonPaths(contents));
            //执行 put
            try (CloseableHttpResponse response = client.execute(put)) {
                HttpEntity httpEntity = response.getEntity();
                String loadResult = EntityUtils.toString(httpEntity);
                int statusCode = response.getStatusLine().getStatusCode();
                if (statusCode == 200 && loadResult.contains("OK") && loadResult.contains("Success")) {
                    log.info("stream load sunccess, loadResult: {}", loadResult);
                } else {
                    log.error("stream load error, statusCode: {}, loadResult: {}", statusCode, loadResult);
                }
            }
        } catch (Exception e) {
            log.error("stream load error", e);
        }
    }

    private String basicAuthHeader(String username, String password) {
        final String tobeEncode = username + ":" + password;
        byte[] encoded = Base64.encodeBase64(tobeEncode.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encoded);
    }

    private String parseColumns(List<Map<String, Object>> columnMaps) {
        Map<String, Object> columnMap = columnMaps.get(0);
        return columnMap.keySet().parallelStream()
                .collect(Collectors.joining(","));
    }

    private String parseJsonPaths(List<Map<String, Object>> columnMaps) {
        Map<String, Object> columnMap = columnMaps.get(0);
        return columnMap.keySet().parallelStream()
                .map(e -> "\"$." + e + "\"")
                .collect(Collectors.joining(",", "[", "]"));
    }
}
