package com.liang.study.test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class Test {
    public static void main(String[] args) {
        Map<Integer, Integer> dataMap = new HashMap<>();
        Random r = new Random();
        while (true) {
            int i = r.nextInt();
            dataMap.put(i, i);
            System.out.println(dataMap.size());
        }
    }
}