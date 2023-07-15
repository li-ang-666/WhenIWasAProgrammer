package com.liang.repair.test;

import com.liang.common.dto.Config;
import com.liang.common.dto.config.FlinkConfig;
import com.liang.common.util.JsonUtils;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Date;
import java.util.HashMap;

public class Test {
    public static void main(String[] args) throws Exception {
        Date date = new Date(System.currentTimeMillis());

        System.out.println(JsonUtils.toString(date));
    }
}
