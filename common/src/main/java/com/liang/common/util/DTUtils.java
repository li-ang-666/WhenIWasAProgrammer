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
     * current
     */
    public static String currentDatetime() {
        return LocalDateTime
                .now(ZoneOffset.of("+8"))
                .format(DEFAULT_FORMATTER);
    }

    /**
     * current
     */
    public static String currentDate() {
        return LocalDateTime
                .now(ZoneOffset.of("+8"))
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
    }

    /**
     * 秒 -> 标准格式-字符串
     */
    public static String fromUnixTime(long seconds) {
        return fromUnixTime(seconds, DEFAULT_FORMAT);
    }

    /**
     * 秒 -> 自定义格式-字符串
     */
    public static String fromUnixTime(long seconds, String newFormat) {
        return LocalDateTime
                .ofEpochSecond(seconds, 0, ZoneOffset.of("+8"))
                .format(DateTimeFormatter.ofPattern(newFormat));
    }

    /**
     * 标准格式-字符串 -> 秒
     */
    public static long unixTimestamp(String standardDatetime) {
        return unixTimestamp(standardDatetime, DEFAULT_FORMAT);
    }

    /**
     * 自定义格式-字符串 -> 秒
     */
    public static long unixTimestamp(String noStandardDatetime, String oldFormat) {
        return LocalDateTime
                .parse(formatDatetime(noStandardDatetime), DateTimeFormatter.ofPattern(oldFormat))
                .toEpochSecond(ZoneOffset.of("+8"));
    }

    /**
     * 标准格式-字符串 -> 自定义格式-字符串
     */
    public static String dateFormat(String standardDatetime, String newFormat) {
        return dateFormat(standardDatetime, DEFAULT_FORMAT, newFormat);
    }

    /**
     * 自定义格式-字符串 -> 自定义格式-字符串
     */
    public static String dateFormat(String noStandardDatetime, String oldFormat, String newFormat) {
        return LocalDateTime
                .parse(formatDatetime(noStandardDatetime), DateTimeFormatter.ofPattern(oldFormat))
                .format(DateTimeFormatter.ofPattern(newFormat));
    }

    /**
     * 日期加减
     */
    public static String dateAdd(String standardDatetime, int num) {
        return LocalDateTime
                .parse(formatDatetime(standardDatetime), DEFAULT_FORMATTER)
                .plusDays(num)
                .format(DEFAULT_FORMATTER);
    }

    /**
     * 日期加减
     */
    public static String dateSub(String standardDatetime, int num) {
        return dateAdd(standardDatetime, -num);
    }

    /**
     * 日期加减
     */
    public static String getLastNDateTime(int nDays, String format) {
        return dateFormat(dateSub(currentDatetime(), nDays), format);
    }

    /**
     * 返回的一定是`yyyy-MM-dd HH:mm:ss` 19位
     */
    private static String formatDatetime(String standardDatetime) {
        return standardDatetime + (standardDatetime.length() == 10 ? " 00:00:00" : "");
    }
}
