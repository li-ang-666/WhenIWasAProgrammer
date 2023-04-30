package com.liang.common.service;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StreamLoad {
    /*private HttpClientBuilder httpClientBuilder = HttpClients
            .custom()
            .setRedirectStrategy(new DefaultRedirectStrategy() {
                @Override
                protected boolean isRedirectable(String method) {
                    return true;
                }
            });

    public void send(StreamLoadDto dto) throws Exception {
        String host = dto.getHost();
        int httpPort = dto.getHttpPort();
        String user = dto.getUser();
        String pwd = dto.getPwd();
        String database = dto.getDatabase();
        String table = dto.getTable();
        String data = dto.getData();
        Lists<String> schema = dto.getSchema();
        try (CloseableHttpClient client = httpClientBuilder.build()) {
            //创建 put
            HttpPut put = new HttpPut(String.format("http://%s:%s/api/%s/%s/_stream_load",
                    host, httpPort, database, table));
            //填充 put
            put.setHeader("Expect", "100-continue");
            String tobeEncode = user + ":" + pwd;
            byte[] encoded = Base64.encodeBase64(tobeEncode.getBytes(StandardCharsets.UTF_8));
            put.setHeader("Authorization", "Basic " + new String(encoded));
            put.setHeader("label", String.valueOf(System.currentTimeMillis()));
            put.setHeader("format", "json");
            put.setHeader("strip_outer_array", "true");
            put.setHeader("merge_type", "MERGE");
            put.setHeader("delete", "del is not null and del = 1");
            put.setHeader("exec_mem_limit", String.valueOf(1024 * 1024 * 1024));
            put.setHeader("max_filter_ratio", "0");
            put.setEntity(new StringEntity(data, StandardCharsets.UTF_8));
            put.setHeader("columns", schema.map(SqlUtils::formatField).mkString("", ",", ""));
            put.setHeader("jsonpaths", schema.map(col -> "\"$." + col + "\"").mkString("[", ",", "]"));
            //执行 put
            try (CloseableHttpResponse response = client.execute(put)) {
                String loadResult = Optional.ofNullable(response.getEntity())
                        .map(functionEraser(EntityUtils::toString))
                        .orElse("");
                int statusCode = response.getStatusLine().getStatusCode();
                if (!(statusCode == 200 && loadResult.contains("OK") && loadResult.contains("Success"))) {
                    log.info("{} 写入异常, statusCode: {}, loadResult: {}, data: {}", table, statusCode, loadResult, data);
                }
            }
        }
    }*/
}
