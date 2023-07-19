package com.liang.repair.test;

import com.liang.common.util.JsonUtils;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class Test {
    public static void main(String[] args) throws Exception {
        // {"id":"1","name":"tom"}
        byte[] bytes = "{\"id\":\"1\",\"name\":\"tom\"}".getBytes(StandardCharsets.UTF_8);
        System.out.println(Arrays.toString(bytes));
        System.out.println(JsonUtils.parseJsonObj("{\"id\":\"1\",\"name\":\"tom\"}"));

    }
}
