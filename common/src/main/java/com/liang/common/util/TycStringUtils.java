package com.liang.common.util;

import lombok.experimental.UtilityClass;

@UtilityClass
public class TycStringUtils {
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
        if (shareholderId == null || shareholderId.length() < 17) {
            return false;
        }
        for (char c : shareholderId.toCharArray()) {
            if (!Character.isLetterOrDigit(c)) {
                return false;
            }
        }
        return true;
    }

    public static boolean isDateTime(String str) {
        return str.matches("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}$");
    }
}
