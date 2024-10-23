package com.liang.flink.test;

import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

@Slf4j
public class FlinkTest {
    public static void main(String[] args) {
        Map<String, Object> map1 = new HashMap<String, Object>() {{
            put("1", "1");
            put("2", "2");
        }};
        Map<String, Object> map2 = new LinkedHashMap<String, Object>() {{
            put("1", "1");
            put("2", "2");
        }};
        System.out.println(map1.equals(map2));
    }
}
