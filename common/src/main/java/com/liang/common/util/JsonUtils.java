package com.liang.common.util;

import com.fasterxml.jackson.core.JsonGenerator;
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
import java.util.TimeZone;

@Slf4j
@SuppressWarnings("unchecked")
@UtilityClass
public class JsonUtils {
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .setTimeZone(TimeZone.getTimeZone("GTM+8"))
            .configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, true)
            .configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true)
            .configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false).setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"))
            //识别控制字符
            .configure(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS.mappedFeature(), true)
            //识别不认识的控制字符
            .configure(JsonReadFeature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER.mappedFeature(), true)
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

    /*----------------------------------序列化------------------------------------------------------*/
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
