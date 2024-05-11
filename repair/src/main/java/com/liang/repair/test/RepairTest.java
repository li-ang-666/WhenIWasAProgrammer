package com.liang.repair.test;

import com.liang.repair.service.ConfigHolder;
import lombok.extern.slf4j.Slf4j;

import java.text.DecimalFormat;

@Slf4j
public class RepairTest extends ConfigHolder {
    public static void main(String[] args) throws Exception {
        DecimalFormat format = new DecimalFormat("#,###");
        System.out.println(format.format(21000000L));

        System.out.println(String.format("%s, %,d", "aaa", 21000000L));
    }
}
