package com.liang.common.util;

import lombok.extern.slf4j.Slf4j;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.Map;

@SuppressWarnings("unchecked")
@Slf4j
public class YamlUtils {
    private YamlUtils() {
    }

    public static Map<String, Object> parse(InputStream inputStream) {
        return parse(inputStream, Map.class);
    }

    public static <T> T parse(InputStream inputStream, Class<T> clz) {
        T res = null;
        try {
            res = new Yaml().loadAs(inputStream, clz);
        } catch (Exception e) {
            log.error("YamlUtils Error", e);
        }
        return res;
    }
}
