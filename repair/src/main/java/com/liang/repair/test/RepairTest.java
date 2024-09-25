package com.liang.repair.test;

import cn.hutool.core.util.IdUtil;
import com.liang.repair.service.ConfigHolder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RepairTest extends ConfigHolder {
    public static void main(String[] args) {
        System.out.println(IdUtil.getSnowflakeNextId() / 10000);
    }
}
