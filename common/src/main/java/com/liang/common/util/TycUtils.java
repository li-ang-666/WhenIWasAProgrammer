package com.liang.common.util;

import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;

import java.math.BigDecimal;
import java.math.RoundingMode;

import static com.liang.common.util.SqlUtils.formatValue;

@UtilityClass
public class TycUtils {
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

    public static boolean isTycUniqueEntityId(String shareholderId) {
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

    public static boolean isValidName(String str) {
        return StringUtils.isNotBlank(str) && !"null".equalsIgnoreCase(str);
    }

    public static boolean isDateTime(String str) {
        return String.valueOf(str).matches("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}$");
    }

    public static Tuple2<String, String> formatEquity(String equity) {
        if (equity == null || equity.isEmpty()) {
            return Tuple2.of(getDecimalString("0", 1), "人民币");
        }
        StringBuilder numberBuilder = new StringBuilder();
        StringBuilder unitBuilder = new StringBuilder();
        int length = equity.length();
        for (int i = 0; i < length; i++) {
            char c = equity.charAt(i);
            if (Character.isDigit(c) || '.' == c) {
                numberBuilder.append(c);
            } else if (!Character.isWhitespace(c) && String.valueOf(c).matches("[\u4e00-\u9fa5]")) {
                unitBuilder.append(c);
            }
        }
        String number = numberBuilder.toString();
        String unit = unitBuilder.toString();
        if (unit.isEmpty()) {
            if (getDecimalString(number, 1).compareTo("10000") >= 0) {
                unit = "元";
            } else {
                unit = "万元";
            }
        } else if ("万".equals(unit)) {
            unit = "万元";
        }
        number = getDecimalString(number, unit.contains("万") ? 10000 * 10 * 10 : 10 * 10);
        if (unit.contains("人民币") || "万元".equals(unit) || "元".equals(unit)) {
            return Tuple2.of(number, "人民币");
        } else if (unit.contains("美国") || unit.contains("美元") || unit.contains("美币")) {
            return Tuple2.of(number, "美元");
        } else if (unit.contains("香港") || unit.contains("港元") || unit.contains("港币")) {
            return Tuple2.of(number, "港元");
        } else if (unit.contains("澳门") || unit.contains("澳元") || unit.contains("澳币")) {
            return Tuple2.of(number, "澳门元");
        } else if (unit.contains("台湾") || unit.contains("台元") || unit.contains("台币")) {
            return Tuple2.of(number, "新台币");
        } else if (unit.contains("日本") || unit.contains("日元") || unit.contains("日币")) {
            return Tuple2.of(number, "日元");
        } else if (unit.contains("欧洲") || unit.contains("欧元") || unit.contains("欧币")) {
            return Tuple2.of(number, "欧元");
        } else if (unit.contains("英国") || unit.contains("英镑") || unit.contains("英元") || unit.contains("英币")) {
            return Tuple2.of(number, "英镑");
        } else if (unit.contains("韩国") || unit.contains("韩元") || unit.contains("韩币")) {
            return Tuple2.of(number, "韩元");
        } else {
            return Tuple2.of(number, unit.replaceAll("万元|万", ""));
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

    public static Tuple2<String, String> companyCid2GidAndName(String companyCid) {
        String sql = new SQL().SELECT("graph_id,name")
                .FROM("enterprise")
                .WHERE("deleted = 0")
                .WHERE("id = " + formatValue(companyCid))
                .toString();
        Tuple2<String, String> res = new JdbcTemplate("464prism").queryForObject(sql, rs -> Tuple2.of(rs.getString(1), rs.getString(2)));
        return res != null ? res : Tuple2.of("0", "");
    }

    public static String humanCid2Gid(String humanCid) {
        String sql = new SQL().SELECT("graph_id")
                .FROM("human_graph")
                .WHERE("deleted = 0")
                .WHERE("human_id = " + formatValue(humanCid))
                .toString();
        String res = new JdbcTemplate("116prism").queryForObject(sql, rs -> rs.getString(1));
        return res != null ? res : "0";
    }

    public static String getHumanHashId(String companyGid, String humanGid) {
        String sql = new SQL().SELECT("human_pid")
                .FROM("company_human_relation")
                .WHERE("deleted = 0")
                .WHERE("company_graph_id = " + formatValue(companyGid))
                .WHERE("human_graph_id = " + formatValue(humanGid))
                .toString();
        String res = new JdbcTemplate("prismBoss").queryForObject(sql, rs -> rs.getString(1));
        return res != null ? res : "0";
    }
}
