package com.liang.repair.impl;

import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.repair.service.ConfigHolder;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class ColumnTypeMapping extends ConfigHolder {
    public static void main(String[] args) {
        // config
        JdbcTemplate jdbcTemplate = new JdbcTemplate("463.bdp_equity");
        String tableName = "entity_controller_details";
        // mapping
        AtomicInteger maxColumnLength = new AtomicInteger(Integer.MIN_VALUE);
        List<Tuple2<String, String>> list = jdbcTemplate.queryForList("desc " + tableName, rs -> {
            String columnName = rs.getString(1);
            String columnType = rs.getString(2);
            maxColumnLength.set(Math.max(maxColumnLength.get(), columnName.length()));
            return Tuple2.of(columnName, mappingToFlinkSqlType(columnType));
        });
        String createTable = list.stream().map(e -> e.f0 + StringUtils.repeat(" ", maxColumnLength.get() + 1 - e.f0.length()) + e.f1)
                .collect(Collectors.joining(",\n", "", ","));
        list.add(Tuple2.of("op_ts", "TIMESTAMP(3)"));
        String sql = list.stream().map(e -> {
            if (e.f1.equals("TIMESTAMP(3)"))
                return String.format("CAST(CONVERT_TZ(CAST(%s AS STRING), 'Asia/Shanghai', 'UTC') AS TIMESTAMP(3)) %s", e.f0, e.f0);
            else
                return e.f0;
        }).collect(Collectors.joining(",", "insert into dwd select\n", "\nfrom ods"));
        System.out.println(StringUtils.repeat("-", 100));
        System.out.println(createTable);
        System.out.println(StringUtils.repeat("-", 100));
        System.out.println(sql);
        System.out.println(StringUtils.repeat("-", 100));
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
