package com.liang.repair.test;

import com.liang.common.util.ObjectSizeCalculator;
import com.liang.repair.service.ConfigHolder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Test extends ConfigHolder {
    @SneakyThrows
    public static void main(String[] args) throws Exception {
        System.out.println(ObjectSizeCalculator.getObjectSize("a"));
        System.out.println(ObjectSizeCalculator.getObjectSize("aa"));
        System.out.println(ObjectSizeCalculator.getObjectSize("aaa"));
        System.out.println(ObjectSizeCalculator.getObjectSize("aaaa"));
        System.out.println(ObjectSizeCalculator.getObjectSize("aaaaa"));
        System.out.println(ObjectSizeCalculator.getObjectSize("aaaaaa"));
    }
}
