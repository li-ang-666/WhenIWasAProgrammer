package com.liang.common.util;

import lombok.experimental.UtilityClass;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

@UtilityClass
public class DateTimeUtils {
    /**
     * 返回某一天是星期几
     */
    public static int getWeek(String date) {
        return LocalDate
                .parse(date.substring(0, 10))
                .getDayOfWeek()
                .getValue();
    }

    public static String fromUnixTime(long seconds) {
        return fromUnixTime(seconds, "yyyy-MM-dd HH:mm:ss");
    }

    public static String fromUnixTime(long seconds, String format) {
        return LocalDateTime
                .ofEpochSecond(seconds, 0, ZoneOffset.of("+8"))
                .format(DateTimeFormatter.ofPattern(format));
    }

    public static long unixTimestamp(String datetime) {
        return unixTimestamp(datetime, "yyyy-MM-dd HH:mm:ss");
    }

    public static long unixTimestamp(String datetime, String format) {
        return LocalDateTime
                .parse(datetime, DateTimeFormatter.ofPattern(format))
                .toEpochSecond(ZoneOffset.of("+8"));
    }

    public static long currentTimestamp() {
        return System.currentTimeMillis() / 1000;
    }

    public static String currentDatetime() {
        return LocalDateTime
                .now(ZoneOffset.of("+8"))
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    public static String currentDate() {
        return LocalDateTime
                .now(ZoneOffset.of("+8"))
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
    }

    public static String currentTime() {
        return LocalDateTime
                .now(ZoneOffset.of("+8"))
                .format(DateTimeFormatter.ofPattern("HH:mm:ss"));
    }

    public static String dateAdd(String date, int num) {
        return LocalDate
                .parse(date.substring(0, 10))
                .plusDays(num).toString();
    }

    public static String dateSub(String date, int num) {
        return LocalDate
                .parse(date.substring(0, 10))
                .minusDays(num).toString();
    }

    /**
     * 获取两个日期间的每一天
     */
    public static List<String> getDays(String startDate, String endDate) {
        String start = startDate.substring(0, 10);
        String end = endDate.substring(0, 10);
        ArrayList<String> days = new ArrayList<>();
        while (start.compareTo(end) <= 0) {
            days.add(start);
            start = dateAdd(start, 1);
        }
        return days;
    }

    public static String getLastNDateTime(int nDays, String format) {
        return fromUnixTime((System.currentTimeMillis() - (nDays * 24L * 3600L * 1000L)) / 1000, format);
    }

    public static long getLastNTimestamp(int nDays) {
        return (System.currentTimeMillis() - (nDays * 24L * 3600L * 1000L)) / 1000;
    }
}
