package com.liang.common.util;

import cn.hutool.core.util.StrUtil;
import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.*;
import java.util.stream.Collectors;

@UtilityClass
public class SqlUtils {
    public static String formatField(String filedName) {
        return "`" + filedName.replaceAll("\\.", "`.`") + "`";
    }


    public static String formatValue(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Collection) {
            return ((Collection<?>) value).parallelStream()
                    .map(SqlUtils::formatValue)
                    .collect(Collectors.joining(",", "(", ")"));
        } else if (value instanceof Boolean) {
            return (boolean) value ? "1" : "0";
        } else if (value instanceof Number || StringUtils.isNumeric(value.toString())) {
            return "'" + value + "'";
        } else {
            return escapeValue(value.toString());
        }
    }

    public static Tuple2<String, String> columnMap2Insert(Map<String, Object> columnMap) {
        if (columnMap == null || columnMap.isEmpty()) {
            return null;
        }
        ArrayList<String> columns = new ArrayList<>();
        ArrayList<String> values = new ArrayList<>();
        for (Map.Entry<String, Object> entry : columnMap.entrySet()) {
            columns.add(formatField(entry.getKey()));
            values.add(formatValue(entry.getValue()));
        }
        return Tuple2.of(
                String.join(", ", columns),
                String.join(", ", values)
        );
    }

    public static Tuple2<String, String> columnMap2Insert(List<Map<String, Object>> columnMaps) {
        if (columnMaps == null || columnMaps.isEmpty()) {
            return null;
        }
        ArrayList<String> formatKeys = new ArrayList<>();
        ArrayList<String> keys = new ArrayList<>(columnMaps.get(0).keySet());
        for (String key : keys) {
            formatKeys.add(formatField(key));
        }
        ArrayList<String> oneRowValues = new ArrayList<>();
        ArrayList<String> allRowValues = new ArrayList<>();
        for (Map<String, Object> columnMap : columnMaps) {
            for (String key : keys) {
                oneRowValues.add(formatValue(columnMap.get(key)));
            }
            allRowValues.add(String.join(", ", oneRowValues));
            oneRowValues.clear();
        }
        return Tuple2.of(
                String.join(", ", formatKeys),
                String.join("), (", allRowValues)
        );
    }

    public static String columnMap2Where(Map<String, Object> columnMap) {
        if (columnMap == null || columnMap.isEmpty()) {
            return null;
        }
        ArrayList<String> res = new ArrayList<>();
        for (Map.Entry<String, Object> entry : columnMap.entrySet()) {
            String syntax;
            String column = entry.getKey();
            Object value = entry.getValue();
            if (value == null) {
                syntax = formatField(column) + " is null";
            } else {
                syntax = formatField(column) + " = " + formatValue(value);
            }
            res.add(syntax);
        }
        return String.join(" and ", res);
    }

    public static String columnMap2Update(Map<String, Object> columnMap) {
        if (columnMap == null || columnMap.isEmpty()) {
            return null;
        }
        ArrayList<String> res = new ArrayList<>();
        for (Map.Entry<String, Object> entry : columnMap.entrySet()) {
            String syntax = formatField(entry.getKey()) + " = " + formatValue(entry.getValue());
            res.add(syntax);
        }
        return String.join(", ", res);
    }

    public static String columnList2Create(List<String> columnList) {
        if (columnList == null || columnList.isEmpty()) {
            return null;
        }
        ArrayList<String> res = new ArrayList<>();
        for (String column : columnList) {
            res.add(formatField(column) + " varchar(255)");
        }
        return String.join(", ", res);
    }

    private static String escapeValue(String value) {
        if (value == null) {
            return null;
        }
        StringBuilder stringBuilder = new StringBuilder(value.length() * 2);
        stringBuilder.append("'");
        char[] chars = value.toCharArray();
        for (char c : chars) {
            switch (c) {
                case '\t':
                    stringBuilder.append("\\t");
                    break;
                case '\r':
                    stringBuilder.append("\\r");
                    break;
                case '\n':
                    stringBuilder.append("\\n");
                    break;
                case '\\':
                    stringBuilder.append("\\\\");
                    break;
                case '\"':
                    stringBuilder.append("\\\"");
                    break;
                case '\'':
                    stringBuilder.append("\\'");
                    break;
                default:
                    stringBuilder.append(c);
                    break;
            }
        }
        return stringBuilder.append("'").toString();
    }

    public String onDuplicateKeyUpdate(String... columns) {
        return onDuplicateKeyUpdate(Arrays.asList(columns));
    }

    public String onDuplicateKeyUpdate(Collection<String> columns) {
        List<String> syntaxList = new ArrayList<>();
        for (String column : columns) {
            if (StrUtil.equalsAny(column, "create_time", "update_time")) {
                continue;
            }
            syntaxList.add(String.format("%s = VALUES(%s)", column, column));
        }
        return " ON DUPLICATE KEY UPDATE " + String.join(",", syntaxList);
    }
}