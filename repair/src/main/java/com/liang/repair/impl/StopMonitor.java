package com.liang.repair.impl;

import cn.hutool.http.HttpRequest;
import cn.hutool.http.HttpResponse;
import cn.hutool.http.HttpUtil;
import com.liang.common.util.JsonUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StopMonitor {
    private final static Map<String, Object> TEMPLATE_MAP = new HashMap<>();

    static {
        TEMPLATE_MAP.put("sshPassWord", "flink_tyc");
        TEMPLATE_MAP.put("isMonitored", 0);
    }

    public static void main(String[] args) {
        HttpRequest post = HttpUtil.createPost("http://10.99.205.87:8990/flink/cancelMonitor");
        String user = "liguangwei";
        List<String> jobs = Arrays.asList(
                "company_equity_relation_details_prod"
        );
        jobs.forEach(job -> {
            HashMap<String, Object> map = new HashMap<>(TEMPLATE_MAP);
            map.put("sshUserName", user);
            map.put("yarnName", job);
            HttpResponse response = post.body(JsonUtils.toString(map), "application/json").execute();
            System.out.println(response.body());
        });
    }
}
