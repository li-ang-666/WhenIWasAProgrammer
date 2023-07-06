package com.liang.common.util;

import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.StringUtils;

import java.util.Random;

@UtilityClass
public class TableNameUtils {
    private static final ThreadLocal<Random> TL = new ThreadLocal<>();

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

    public static String getRandomTableName() {
        if (TL.get() == null) {
            TL.set(new Random());
        }
        return String.format("t_%s_%s",
                System.currentTimeMillis(),
                TL.get().nextInt(Integer.MAX_VALUE));
    }
}
