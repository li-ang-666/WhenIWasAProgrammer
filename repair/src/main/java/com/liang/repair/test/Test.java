package com.liang.repair.test;

import com.liang.common.dto.Config;
import com.liang.common.dto.config.FlinkConfig;
import com.liang.common.util.JsonUtils;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class Test {
    public static void main(String[] args) throws Exception {
        Map<String, Object> map = JsonUtils.parseJsonObj("{\"id\":1.1000000000000000000000,\"name\":\"tom\"}");
        map.forEach((k,v)->{
            System.out.println(k+" -> "+v);
            System.out.println(k.getClass());
            System.out.println(v.getClass());
        });

        System.out.println(JsonUtils.toString(new BigDecimal("3E10")));
    }
}
