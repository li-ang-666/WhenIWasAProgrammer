package com.liang.repair.test;

import com.liang.repair.service.ConfigHolder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;

@Slf4j
public class Test extends ConfigHolder {
    @SneakyThrows
    public static void main(String[] args) throws Exception {
        System.out.println(TimeUnit.DAYS.toMillis(5));
    }
}
