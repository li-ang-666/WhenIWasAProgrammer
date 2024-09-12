package com.liang.repair.test;

import cn.hutool.system.SystemUtil;
import com.liang.common.util.JsonUtils;
import com.liang.repair.service.ConfigHolder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RepairTest extends ConfigHolder {
    public static void main(String[] args) throws Exception {
        System.out.println(JsonUtils.toString(SystemUtil.getOperatingSystemMXBean()));
        System.out.println(JsonUtils.toString(SystemUtil.getRuntimeMXBean()));
        System.out.println(JsonUtils.toString(SystemUtil.getThreadMXBean()));
    }
}
