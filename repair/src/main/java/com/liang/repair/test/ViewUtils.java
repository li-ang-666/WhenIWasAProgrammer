package com.liang.repair.test;

import com.alibaba.druid.pool.DruidDataSource;
import com.liang.common.util.SqlUtils;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ViewUtils {
    private static final DruidDataSource druidDataSource;

    static {
        druidDataSource = new DruidDataSource();
        druidDataSource.setTestWhileIdle(false);
        druidDataSource.setDriverClassName("org.hsqldb.jdbc.JDBCDriver");
        druidDataSource.setUrl("jdbc:hsqldb:mem:db");
        druidDataSource.setUsername("user");
        druidDataSource.setPassword("password");
    }

    private static void registerView(List<Map<String, Object>> rows, List<String> schema) {
        try (Connection connection = druidDataSource.getConnection()) {
            String tableName = "t" + System.currentTimeMillis();
            StringBuilder builder = new StringBuilder(String.format("create table %s(", tableName));
            for (String col : schema) {
                builder.append(col).append(" ").append("varchar(255),");
            }
            builder.deleteCharAt(builder.length() - 1);
            builder.append(")");
            connection.prepareStatement(builder.toString()).execute();
            for (Map<String, Object> row : rows) {
                String sql = String.format("insert into %s(%s)values(%s)", tableName,
                        String.join(",", schema),
                        schema.parallelStream().map(e -> SqlUtils.formatValue(row.get(e))).collect(Collectors.joining(",")));
                connection.prepareStatement(sql).execute();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        ArrayList<String> list = new ArrayList<>();
        list.add("name");
        list.add("id");


        ArrayList<Map<String, Object>> columnMaps = new ArrayList<>();
        HashMap<String, Object> map = new HashMap<>();
        map.put("tb.id", "1");
        map.put("tb.name", "AAA");
        map.put("tb.info", null);
        /*System.out.println(SqlUtils.columnMap2Insert(map).f0);
        System.out.println(SqlUtils.columnMap2Insert(map).f1);
        System.out.println(SqlUtils.columnMap2Where(map));
        System.out.println(SqlUtils.columnMap2Update(map));*/
        map = new HashMap<>();
        map.put("tb.id", "2");
        map.put("tb.name", "BBB");
        map.put("tb.info", "abcdef");
        columnMaps.add(map);
        map = new HashMap<>();
        map.put("tb.id", "3");
        map.put("tb.name", "CCC");
        map.put("tb.info", null);
        columnMaps.add(map);
        map = new HashMap<>();
        map.put("tb.id", "4");
        map.put("tb.name", "DDD");
        map.put("tb.info", 244565645);
        columnMaps.add(map);
        System.out.println(SqlUtils.columnMap2Insert(columnMaps).f0);
        System.out.println(SqlUtils.columnMap2Insert(columnMaps).f1);
    }
}
