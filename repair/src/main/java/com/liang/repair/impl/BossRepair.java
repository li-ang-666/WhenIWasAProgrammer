package com.liang.repair.impl;

import cn.hutool.http.HttpUtil;
import com.liang.repair.service.ConfigHolder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BossRepair extends ConfigHolder {
    public static void main(String[] args) {
        String body = HttpUtil.createPost("http://10.99.138.255:9060/update_boss")
                .form("hgid", 1964456817)
                .execute()
                .body();
        System.out.println(body);
    }
}
