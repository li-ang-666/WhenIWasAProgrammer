package com.liang.common.util;

import lombok.SneakyThrows;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.Map;

@SuppressWarnings("unchecked")
public class YamlUtils {
    private YamlUtils() {
    }

    public static Map<String, Object> fromResource(String fileName) {
        return fromResource(fileName, Map.class);
    }

    @SneakyThrows
    public static <T> T fromResource(String fileName, Class<T> clz) {
        try (InputStream inputStream = IOUtils.getInStreamFromResource(fileName)) {
            return new Yaml().loadAs(inputStream, clz);
        }
    }

    public static Map<String, Object> fromDisk(String path) {
        return fromDisk(path, Map.class);
    }

    @SneakyThrows
    public static <T> T fromDisk(String path, Class<T> clz) {
        try (InputStream inputStream = IOUtils.getInStreamFromDisk(path)) {
            return new Yaml().loadAs(inputStream, clz);
        }
    }
}
