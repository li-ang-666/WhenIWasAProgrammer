package com.liang.common.util;

import lombok.experimental.UtilityClass;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@UtilityClass
public class DTUtils {
    private static final String DEFAULT_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private static final DateTimeFormatter DEFAULT_FORMATTER = DateTimeFormatter.ofPattern(DEFAULT_FORMAT);

    /**
     * 返回的一定是`yyyy-MM-dd HH:mm:ss` 19位
     */
    private static String formatDatetime(String datetime) {
        return datetime + (datetime.length() == 10 ? " 00:00:00" : "");
    }

    /**
     * 字符串 -> 字符串
     */
    public static String dateFormat(String datetime, String format) {
        return LocalDateTime
                .parse(formatDatetime(datetime), DEFAULT_FORMATTER)
                .format(DateTimeFormatter.ofPattern(format));
    }

    /**
     * 秒 -> 字符串
     */
    public static String fromUnixTime(long seconds) {
        return fromUnixTime(seconds, DEFAULT_FORMAT);
    }

    /**
     * 秒 -> 字符串
     */
    public static String fromUnixTime(long seconds, String format) {
        return LocalDateTime
                .ofEpochSecond(seconds, 0, ZoneOffset.of("+8"))
                .format(DateTimeFormatter.ofPattern(format));
    }

    /**
     * 字符串 -> 秒
     */
    public static long unixTimestamp(String datetime) {
        return unixTimestamp(datetime, DEFAULT_FORMAT);
    }

    /**
     * 字符串 -> 秒
     */
    public static long unixTimestamp(String datetime, String format) {
        return LocalDateTime
                .parse(formatDatetime(datetime), DateTimeFormatter.ofPattern(format))
                .toEpochSecond(ZoneOffset.of("+8"));
    }

    public static String currentDatetime() {
        return LocalDateTime
                .now(ZoneOffset.of("+8"))
                .format(DEFAULT_FORMATTER);
    }

    public static String currentDate() {
        return LocalDateTime
                .now(ZoneOffset.of("+8"))
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
    }

    public static String dateAdd(String datetime, int num) {
        return LocalDateTime
                .parse(formatDatetime(datetime))
                .plusDays(num).toString();
    }

    public static String dateSub(String datetime, int num) {
        return dateAdd(datetime, -num);
    }

    public static String getLastNDateTime(int nDays, String format) {
        return dateFormat(dateSub(currentDatetime(), nDays), format);
    }
}
