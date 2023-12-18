package com.liang.repair.test;

import cn.hutool.http.HttpUtil;
import com.liang.repair.service.ConfigHolder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Test extends ConfigHolder {
    public static void main(String[] args) {
        String res = HttpUtil.createPost("http://10.99.199.173:10040/linking_yuqing_rank")
                .form("text", "")
                .form("bid_uuid", "123")
                .execute()
                .body();
        System.out.println(res);
    }
}
