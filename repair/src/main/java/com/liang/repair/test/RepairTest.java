package com.liang.repair.test;

import com.liang.common.util.DTUtils;
import com.liang.repair.service.ConfigHolder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RepairTest extends ConfigHolder {
    public static void main(String[] args) throws Exception {
        System.out.println(DTUtils.fromUnixTime(System.currentTimeMillis() / 1000));
        System.out.println(DTUtils.fromUnixTime(System.currentTimeMillis() / 1000, "yyyyMMddHHmmss"));
        System.out.println(DTUtils.unixTimestamp("2024-01-01"));
        System.out.println(DTUtils.unixTimestamp("2024-01-01 00:01:00"));
        System.out.println(DTUtils.currentDate());
        System.out.println(DTUtils.currentDatetime());
        System.out.println(DTUtils.dateAdd(DTUtils.currentDate(), 1));
        System.out.println(DTUtils.dateAdd(DTUtils.currentDatetime(), 1));
        System.out.println(DTUtils.dateSub(DTUtils.currentDate(), 1));
        System.out.println(DTUtils.dateSub(DTUtils.currentDatetime(), 1));
        System.out.println(DTUtils.getLastNDateTime(1, "yyyyMMdd"));
    }
}
