package com.liang.repair.test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class Test {
    public static void main(String[] args) throws Exception {
        //{"id":1}
        byte[] bytes = "{\"id\":1}".getBytes(StandardCharsets.UTF_8);
        System.out.println(Arrays.toString(bytes));
    }
}
