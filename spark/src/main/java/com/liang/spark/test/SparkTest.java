package com.liang.spark.test;

import com.liang.common.util.JsonUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;

@Slf4j
public class SparkTest {
    public static void main(String[] args) throws Exception {
        System.out.println(JsonUtils.toString(new HashMap<>()));
    }
}
