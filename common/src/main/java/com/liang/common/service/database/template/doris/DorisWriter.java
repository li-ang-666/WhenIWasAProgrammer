package com.liang.common.service.database.template.doris;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import com.liang.common.dto.DorisOneRow;
import com.liang.common.dto.DorisSchema;
import com.liang.common.dto.config.DorisConfig;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;

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
import java.util.stream.Collectors;

import static org.apache.http.HttpHeaders.AUTHORIZATION;
import static org.apache.http.HttpHeaders.EXPECT;

@Slf4j
public class DorisWriter {
    private static final byte JSON_PREFIX = (byte) '[';
    private static final byte JSON_SEPARATOR = (byte) ',';
    private static final byte JSON_SUFFIX = (byte) ']';
    private final HttpPutExecutor putExecutor = new HttpPutExecutor();
    private final AtomicInteger fePointer = new AtomicInteger(0);
    private final ByteBuffer buffer;
    private final List<String> fe;
    private final String auth;
    private DorisSchema schema;
    private List<String> keys;

    public DorisWriter(String name, int bufferSize) {
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
            byte[] content = JsonUtils.toBytes(columnMap);
            // 1(separator) + content + 1(suffix)
            if (buffer.position() + (1 + content.length + 1) > buffer.limit()) flush();
            buffer.put(JSON_SEPARATOR);
            buffer.put(content);
        }
    }

    public void flush() {
        synchronized (buffer) {
            if (buffer.position() > 0) {
                buffer.put(0, JSON_PREFIX);
                buffer.put(JSON_SUFFIX);
                HttpPut put = getCommonHttpPut();
                put.setEntity(new ByteArrayEntity(buffer.array(), 0, buffer.position()));
                putExecutor.execute(put, getUri(), getLabel());
            }
            buffer.clear();
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
}
