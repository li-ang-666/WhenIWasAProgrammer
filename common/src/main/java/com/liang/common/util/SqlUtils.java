package com.liang.common.util;

import org.apache.commons.lang3.StringUtils;

public class SqlUtils {
    private SqlUtils() {
    }

    public static String formatField(String filedName) {
        return StringUtils.join("`", filedName, "`");
    }

    public static String formatValue(Object value) {
        if (value == null) {
            return "NULL";
        } else if (value instanceof Boolean) {
            return (Boolean) value ? "1" : "0";
        } else if (value instanceof Number) {
            return String.valueOf(value);
        } else {
            String valueString = String.valueOf(value);
            return isSpecialValue(valueString) ?
                    valueString :
                    escapeValue(valueString);
        }
    }

    private static boolean isSpecialValue(String value) {
        return value.matches("\\d+");
    }

    private static String escapeValue(String value) {
        StringBuilder stringBuilder = new StringBuilder(value.length() * 2);
        stringBuilder.append("\"");
        char[] chars = value.toCharArray();
        for (char c : chars) {
            switch (c) {
                case '\t':
                    stringBuilder.append("\\t"); //insert into test values("\t");
                case '\r':
                    stringBuilder.append("\\r"); //insert into test values("\r");
                case '\n':
                    stringBuilder.append("\\n"); //insert into test values("\n");
                case '\\':
                    stringBuilder.append("\\\\"); //insert into test values("\\");
                case '\"':
                    stringBuilder.append("\\\""); //insert into test values("\"");
                case '\'':
                    stringBuilder.append("\\'"); //insert into test values("\'");
                default:
                    stringBuilder.append(c);
            }
        }
        return stringBuilder.append("\"").toString();
    }
}