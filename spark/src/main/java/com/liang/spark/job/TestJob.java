package com.liang.spark.job;

import com.liang.common.util.JsonUtils;

import java.util.HashMap;

public class TestJob {
    public static void main(String[] args) {
        System.out.println(JsonUtils.toString(new HashMap<String, Object>()));
    }
}
