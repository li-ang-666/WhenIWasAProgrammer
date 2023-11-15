package com.liang.repair.test;

import com.liang.common.util.ObjectSizeCalculator;
import com.liang.repair.service.ConfigHolder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.UUID;

@Slf4j
public class Test extends ConfigHolder {
    public static void main(String[] args) throws Exception {
        System.out.println(ObjectSizeCalculator.getObjectSize(UUID.randomUUID() + StringUtils.repeat(" ", 455)));
    }
}
