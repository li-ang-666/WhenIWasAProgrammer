package com.liang.repair.test;

import cn.hutool.core.exceptions.ExceptionUtil;
import com.liang.repair.service.ConfigHolder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RepairTest extends ConfigHolder {
    public static void main(String[] args) throws Exception {
        RuntimeException runtimeException = new RuntimeException();
        System.out.println(ExceptionUtil.stacktraceToOneLineString(runtimeException));
    }
}
