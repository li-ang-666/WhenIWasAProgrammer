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
    private final static String URL = "http://10.99.205.87:8990/flink/cancelMonitor";
    private final static Map<String, Object> TEMPLATE_MAP = new HashMap<>();

    static {
        TEMPLATE_MAP.put("sshPassWord", "Moka20190520");
        TEMPLATE_MAP.put("isMonitored", 0);
    }

    public static void main(String[] args) {
        HttpRequest post = HttpUtil.createPost(URL);
        String user = "omm";
        List<String> jobs = Arrays.asList(
                "hudi_upsert_company_clean_info",
                "hudi_upsert_company_index",
                "hudi_upsert_company_legal_person",
                "hudi_upsert_company_equity_relation_details",
                "hudi_upsert_personnel",
                "hudi_upsert_senior_executive_hk",
                "hudi_upsert_senior_executive",
                "hudi_upsert_company_bond_plates",
                "hudi_upsert_company_human_relation"
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
