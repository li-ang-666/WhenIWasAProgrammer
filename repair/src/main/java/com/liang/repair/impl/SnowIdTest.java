package com.liang.repair.impl;

import com.liang.repair.service.ConfigHolder;

public class SnowIdTest extends ConfigHolder {

    public static void main(String[] args) {
        for (int i = 1; i <= 2048; i++) {
            int i1 = (i-1) % (32 * 32);
            log.info("dataCenterId: {}, workerId: {}",i1/32,i1%32);
        }
    }
}
