package com.liang.common.util;

import cn.hutool.core.io.resource.ResourceUtil;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

@UtilityClass
@Slf4j
public class AreaCodeUtils {
    private static final Map<String, String> CODE_TO_AREA = new HashMap<>();
    private static final Map<String, String> AREA_TO_CODE = new HashMap<>();

    static {
        String areaCodeJson = ResourceUtil.readStr("area-code.json", StandardCharsets.UTF_8);
        Map<String, Object> areaCodeMap = JsonUtils.parseJsonObj(areaCodeJson);
        areaCodeMap.forEach((k, v) -> {
            CODE_TO_AREA.put(k, (String) v);
            AREA_TO_CODE.put((String) v, k);
        });
    }

    public static String getArea(String code) {
        return CODE_TO_AREA.get(code);
    }

    public static String getCode(String area) {
        return AREA_TO_CODE.get(area);
    }
}
