package com.liang.repair.test;

import com.liang.repair.service.ConfigHolder;

public class Test extends ConfigHolder {
    public static void main(String[] args) throws Exception {
        log.error("abc", (Throwable) null);
    }
}
