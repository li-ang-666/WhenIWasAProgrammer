package com.liang.flink.test;

import org.apache.commons.lang.StringUtils;

import java.util.concurrent.Executors;

public class FlinkTest {
    public static void main(String[] args) {
        String s = "４５８８";
        System.out.println(StringUtils.isNumeric(s));
    }
}
