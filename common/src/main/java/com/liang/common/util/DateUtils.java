package com.liang.common.util;

import lombok.experimental.UtilityClass;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * 用于 date 或者 datetime 的工具类, 不适用于只有 time !!!
 * LocalDateTime 解析字符串的时候, 必须要有年、月、日、时 !!!
 */
@UtilityClass
public class DateUtils {
    private static final String DEFAULT_FORMAT = "yyyy-MM-dd HH:mm:ss";

    /**
     * current
     */
    public static String currentDatetime() {
        return LocalDateTime
                .now(ZoneOffset.of("+8"))
                .format(DateTimeFormatter.ofPattern(DEFAULT_FORMAT));
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
     * 秒 -> 标准格式
     */
    public static String fromUnixTime(long seconds) {
        return fromUnixTime(seconds, DEFAULT_FORMAT);
    }

    /**
     * 秒 -> 自定义格式
     */
    public static String fromUnixTime(long seconds, String newFormat) {
        return LocalDateTime
                .ofEpochSecond(seconds, 0, ZoneOffset.of("+8"))
                .format(DateTimeFormatter.ofPattern(newFormat));
    }

    /**
     * 标准格式 -> 秒
     */
    public static long unixTimestamp(String standardDatetime) {
        standardDatetime = ensureStandard(standardDatetime);
        return unixTimestamp(standardDatetime, DEFAULT_FORMAT);
    }

    /**
     * 自定义格式 -> 秒
     */
    public static long unixTimestamp(String noStandardDatetime, String oldFormat) {
        try {
            return LocalDateTime
                    .parse(noStandardDatetime, DateTimeFormatter.ofPattern(oldFormat))
                    .toEpochSecond(ZoneOffset.of("+8"));
        } catch (Exception ignore) {
            return LocalDateTime
                    .parse(noStandardDatetime + " 00", DateTimeFormatter.ofPattern(oldFormat + " HH"))
                    .toEpochSecond(ZoneOffset.of("+8"));
        }
    }

    /**
     * 标准格式 -> 自定义格式
     */
    public static String dateFormat(String standardDatetime, String newFormat) {
        standardDatetime = ensureStandard(standardDatetime);
        return dateFormat(standardDatetime, DEFAULT_FORMAT, newFormat);
    }

    /**
     * 自定义格式 -> 自定义格式
     */
    public static String dateFormat(String noStandardDatetime, String oldFormat, String newFormat) {
        try {
            return LocalDateTime
                    .parse(noStandardDatetime, DateTimeFormatter.ofPattern(oldFormat))
                    .format(DateTimeFormatter.ofPattern(newFormat));
        } catch (Exception ignore) {
            return LocalDateTime
                    .parse(noStandardDatetime + " 00", DateTimeFormatter.ofPattern(oldFormat + " HH"))
                    .format(DateTimeFormatter.ofPattern(newFormat));
        }
    }

    /**
     * 标准格式 日期加减
     */
    public static String dateAdd(String standardDatetime, int num) {
        standardDatetime = ensureStandard(standardDatetime);
        return LocalDateTime
                .parse(standardDatetime, DateTimeFormatter.ofPattern(DEFAULT_FORMAT))
                .plusDays(num)
                .format(DateTimeFormatter.ofPattern(DEFAULT_FORMAT));
    }

    /**
     * 离线数仓 pt
     */
    public static String getOfflinePt(int nDays, String newFormat) {
        return dateFormat(dateAdd(currentDatetime(), -nDays), newFormat);
    }

    /**
     * 返回的一定是 `yyyy-MM-dd HH:mm:ss` 19位
     */
    private static String ensureStandard(String datetime) {
        return datetime + (datetime.length() == 10 ? " 00:00:00" : "");
    }
}
