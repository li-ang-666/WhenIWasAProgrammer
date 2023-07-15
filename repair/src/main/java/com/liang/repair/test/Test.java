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
        HashMap<String, Object> map = new HashMap<>();
        map.put("1",1);
        map.put("2",2);
        map.put("3",3);
        map.put("4",4);
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            map.remove(entry.getKey());
        }
    }
}
