package com.liang.common.util;

import org.apache.commons.lang3.StringUtils;

public class TableNameUtils {
    private TableNameUtils() {
    }

    public static String humpToUnderLine(String in) {
        if (StringUtils.isBlank(in)) {
            return null;
        }
        int len = in.length();
        StringBuilder builder = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            char c = in.charAt(i);
            if (Character.isUpperCase(c) && i != 0) {
                builder.append("_");
            }
            builder.append(Character.toLowerCase(c));
        }
        return builder.toString();
    }
}
