package com.liang.common.util;

import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.StringUtils;

@UtilityClass
public class TycUtils {

    public static boolean isCompanyId(String companyId) {
        return StringUtils.isNumeric(companyId) && !"0".equals(companyId);
    }

    public static boolean isShareholderId(String shareholderId) {
        if (isCompanyId(shareholderId)) {
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
}
