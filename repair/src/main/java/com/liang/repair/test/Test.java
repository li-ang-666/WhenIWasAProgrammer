package com.liang.repair.test;

import com.liang.repair.service.ConfigHolder;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;

@Slf4j
public class Test extends ConfigHolder {
    public static void main(String[] args) {
        HashMap<String, String> map = new HashMap<String, String>() {{
            //put("key", "abc");
        }};
        System.out.println(map.values().stream().anyMatch(e -> !e.equals("aaa")));
    }
}
