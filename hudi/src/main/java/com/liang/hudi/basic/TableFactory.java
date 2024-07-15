package com.liang.hudi.basic;

import cn.hutool.core.util.IdUtil;
import com.liang.common.dto.Config;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hudi.common.model.WriteOperationType;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@UtilityClass
@Slf4j
public class TableFactory {
    public static void main(String[] args) {
        System.out.println(fromTemplate(WriteOperationType.UPSERT, "435.company_base", "company_index"));
    }

    public static String fromFile(String fileName) {
        try {
            InputStream stream = TableFactory.class.getClassLoader()
                    .getResourceAsStream(fileName);
            assert stream != null;
            return IOUtils.toString(stream, StandardCharsets.UTF_8);
        } catch (Exception e) {
            log.error("create table from file error");
            throw new RuntimeException(e);
        }
    }

    @SneakyThrows
    public static String fromTemplate(WriteOperationType writeOperationType, String source, String tableName) {
        // config
        Config config = ConfigUtils.createConfig(null);
        ConfigUtils.setConfig(config);
        JdbcTemplate jdbcTemplate = new JdbcTemplate(source);
        // calculate
        String min = jdbcTemplate.queryForObject("select min(id) from " + tableName, rs -> rs.getString(1));
        String max = jdbcTemplate.queryForObject("select max(id) from " + tableName, rs -> rs.getString(1));
        // mapping
        AtomicInteger maxColumnLength = new AtomicInteger(Integer.MIN_VALUE);
        List<Tuple2<String, String>> list = jdbcTemplate.queryForList("desc " + tableName, rs -> {
            String columnName = rs.getString(1);
            String columnType = rs.getString(2);
            maxColumnLength.set(Math.max(maxColumnLength.get(), columnName.length()));
            return Tuple2.of(columnName, mappingToFlinkSqlType(columnType));
        });
        String createTable = list.stream().map(e -> "  " + e.f0 + StringUtils.repeat(" ", maxColumnLength.get() + 1 - e.f0.length()) + e.f1 + ",")
                .collect(Collectors.joining("\n", "\n", ""));
        // sql
        list.add(Tuple2.of("op_ts", "TIMESTAMP(3)"));
        String sql = list.stream().map(e -> {
            if (e.f1.equals("TIMESTAMP(3)"))
                return String.format("CAST(CONVERT_TZ(CAST(%s AS STRING), 'Asia/Shanghai', 'UTC') AS TIMESTAMP(3)) %s", e.f0, e.f0);
            else
                return e.f0;
        }).collect(Collectors.joining(", ", "INSERT INTO dwd SELECT\n", "\nFROM ods"));
        // 拼接
        if (writeOperationType == WriteOperationType.BULK_INSERT) {
            InputStream stream = TableFactory.class.getClassLoader()
                    .getResourceAsStream("sql/bulk_insert.sql");
            assert stream != null;
            String template = IOUtils.toString(stream, StandardCharsets.UTF_8);
            return String.format(template, createTable, config.getDbConfigs().get(source).getHost(), config.getDbConfigs().get(source).getDatabase(), tableName, min, max, createTable, tableName, tableName, sql);
        } else if (writeOperationType == WriteOperationType.UPSERT) {
            InputStream stream = TableFactory.class.getClassLoader()
                    .getResourceAsStream("sql/cdc.sql");
            assert stream != null;
            String template = IOUtils.toString(stream, StandardCharsets.UTF_8);
            return String.format(template, createTable, config.getDbConfigs().get(source).getHost(), config.getDbConfigs().get(source).getDatabase(), tableName, 5400 + IdUtil.getSnowflakeNextId() % 1000, createTable, tableName, tableName, sql);
        } else {
            throw new RuntimeException("writeOperationType need to be BULK_INSERT or UPSERT");
        }
    }

    private static String mappingToFlinkSqlType(String mysqlType) {
        // 文本
        if (mysqlType.contains("text") || mysqlType.contains("char")) {
            return "STRING";
        }
        // 日期
        if ("date".equals(mysqlType)) {
            return "DATE";
        }
        if ("datetime".equals(mysqlType)) {
            return "TIMESTAMP(3)";
        }
        // 数字
        if (mysqlType.startsWith("decimal")) {
            return mysqlType.toUpperCase();
        }
        if (mysqlType.startsWith("bigint")) {
            return mysqlType.contains("unsigned") ? "DECIMAL(20, 0)" : "BIGINT";
        }
        if (mysqlType.startsWith("int")) {
            return mysqlType.contains("unsigned") ? "BIGINT" : "INT";
        }
        if (mysqlType.startsWith("smallint")) {
            return mysqlType.contains("unsigned") ? "INT" : "SMALLINT";
        }
        if (mysqlType.startsWith("tinyint")) {
            return mysqlType.contains("unsigned") ? "SMALLINT" : "TINYINT";
        }
        if ("float".equals(mysqlType) || "double".equals(mysqlType)) {
            return mysqlType.toUpperCase();
        }
        // 其它
        return "STRING";
    }
}
