package com.liang.common.util;

import lombok.experimental.UtilityClass;
import org.apache.flink.api.java.tuple.Tuple2;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;

@UtilityClass
public class TycStringUtils {
    private static final List<Character> EQUITY_UNIT_BLACK_CHAR = new ArrayList<>();

    static {
        EQUITY_UNIT_BLACK_CHAR.add(',');
        EQUITY_UNIT_BLACK_CHAR.add('(');
        EQUITY_UNIT_BLACK_CHAR.add(')');
        EQUITY_UNIT_BLACK_CHAR.add('{');
        EQUITY_UNIT_BLACK_CHAR.add('}');
        EQUITY_UNIT_BLACK_CHAR.add('-');
        EQUITY_UNIT_BLACK_CHAR.add('&');
    }

    public static boolean isUnsignedId(String id) {
        if (id == null || id.isEmpty()) {
            return false;
        }
        if ("0".equals(id)) {
            return false;
        }
        int length = id.length();
        for (int i = 0; i < length; i++) {
            if (!Character.isDigit(id.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    public static boolean isShareholderId(String shareholderId) {
        if (isUnsignedId(shareholderId)) {
            return true;
        }
        if (shareholderId == null || shareholderId.isEmpty()) {
            return false;
        }
        int length = shareholderId.length();
        if (length < 17) {
            return false;
        }
        for (int i = 0; i < length; i++) {
            if (!Character.isLetterOrDigit(shareholderId.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    public static boolean isDateTime(String str) {
        return str.matches("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}$");
    }

    public static Tuple2<String, String> formatEquity(String equity) {
        if (equity == null || equity.isEmpty()) {
            return Tuple2.of(
                    new BigDecimal("0").setScale(12, RoundingMode.DOWN).toPlainString(),
                    "");
        }
        StringBuilder numberBuilder = new StringBuilder();
        StringBuilder unitBuilder = new StringBuilder();
        int length = equity.length();
        for (int i = 0; i < length; i++) {
            char c = equity.charAt(i);
            if (Character.isDigit(c) || '.' == c) {
                numberBuilder.append(c);
            } else if (!Character.isWhitespace(c) && !Character.isLetterOrDigit(c) && !EQUITY_UNIT_BLACK_CHAR.contains(c)) {
                unitBuilder.append(c);
            }
        }
        String number = numberBuilder.toString();
        String unit = unitBuilder.toString();
        int multiply = unit.contains("万") ? 10000 * 10 * 10 : 10 * 10;
        if (unit.contains("人民币") || "万元".equals(unit) || "元".equals(unit)) {
            return Tuple2.of(getDecimalString(number, multiply), "人民币");
        } else if (unit.contains("美国") || unit.contains("美元")) {
            return Tuple2.of(getDecimalString(number, multiply), "美元");
        } else if (unit.contains("香港") || unit.contains("港元") || unit.contains("港币")) {
            return Tuple2.of(getDecimalString(number, multiply), "港元");
        } else if (unit.contains("澳门")) {
            return Tuple2.of(getDecimalString(number, multiply), "澳门元");
        } else if (unit.contains("台湾") || unit.contains("台元") || unit.contains("台币")) {
            return Tuple2.of(getDecimalString(number, multiply), "新台币");
        } else if (unit.contains("日本") || unit.contains("日元")) {
            return Tuple2.of(getDecimalString(number, multiply), "日元");
        } else if (unit.contains("欧洲") || unit.contains("欧元") || unit.contains("欧")) {
            return Tuple2.of(getDecimalString(number, multiply), "欧元");
        } else if (unit.contains("英国") || unit.contains("英镑")) {
            return Tuple2.of(getDecimalString(number, multiply), "英镑");
        } else if (unit.contains("韩国") || unit.contains("韩元")) {
            return Tuple2.of(getDecimalString(number, multiply), "韩元");
        } else {
            return Tuple2.of(getDecimalString(number, multiply), unit.replaceAll("万元|万", ""));
        }
    }

    private static String getDecimalString(String number, long multiply) {
        try {
            BigDecimal bigDecimal = new BigDecimal(multiply);
            return new BigDecimal(number)
                    .multiply(bigDecimal)
                    .setScale(12, RoundingMode.DOWN)
                    .toPlainString();
        } catch (Exception e) {
            return new BigDecimal(0)
                    .setScale(12, RoundingMode.DOWN)
                    .toPlainString();
        }
    }
}
