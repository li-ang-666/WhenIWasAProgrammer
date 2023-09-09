package com.liang.repair.test;

import com.liang.common.util.TycUtils;
import com.liang.repair.service.ConfigHolder;

public class Test extends ConfigHolder {
    public static void main(String[] args) throws Exception {
        System.out.println(TycUtils.isDateTime("1880-12-12"));
        System.out.println(TycUtils.isDateTime("2020-02-02"));
        System.out.println(TycUtils.isDateTime("2023-12-31 12:12:12"));
        System.out.println(TycUtils.isDateTime("2024-12-31 12:12:12.999"));
    }
}
