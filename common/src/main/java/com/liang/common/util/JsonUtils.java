package com.liang.common.util;

import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;

@Slf4j
@SuppressWarnings("unchecked")
@UtilityClass
public class JsonUtils {
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false).setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"))
            .configure(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS.mappedFeature(), true)
            .configure(JsonReadFeature.ALLOW_JAVA_COMMENTS.mappedFeature(), true);

    /*----------------------------------解析{XX=XX, XX=XX, XX=XX}----------------------------------*/
    public static Map<String, Object> parseJsonObj(String json) {
        return parseJsonObj(json, Map.class);
    }

    public static <T> T parseJsonObj(String json, Class<T> clz) {
        T t;
        try {
            t = objectMapper.readValue(json, clz);
        } catch (Exception e) {
            log.error("JsonUtils error", e);
            t = null;
        }
        return t;
    }

    /*----------------------------------解析[XX, XX, XX]-------------------------------------------*/
    public static List<Object> parseJsonArr(String json) {
        return parseJsonArr(json, Object.class);
    }

    public static <T> List<T> parseJsonArr(String json, Class<T> clz) {
        List<T> result;
        try {
            JavaType javaType = objectMapper.getTypeFactory().constructParametricType(List.class, clz);
            result = objectMapper.readValue(json, javaType);
        } catch (Exception e) {
            log.error("JsonUtils error", e);
            result = null;
        }
        return result;
    }

    /*----------------------------------反解析------------------------------------------------------*/
    public static String toString(Object o) {
        String result;
        try {
            result = objectMapper.writeValueAsString(o);
        } catch (Exception e) {
            log.error("JsonUtils error", e);
            result = null;
        }
        return result;
    }
}
