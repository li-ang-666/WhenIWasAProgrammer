package com.liang.repair.test;

import java.util.HashMap;
import java.util.Map;

public class Test {
    public static void main(String[] args) throws Exception {
        HashMap<String, Object> map = new HashMap<>();
        map.put("1", 1);
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            map.remove(entry.getKey());
        }
    }
}
