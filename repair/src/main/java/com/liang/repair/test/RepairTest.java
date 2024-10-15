package com.liang.repair.test;

import cn.hutool.core.exceptions.ExceptionUtil;
import com.liang.repair.service.ConfigHolder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RepairTest extends ConfigHolder {
    public static void main(String[] args) throws Exception {
        Exception exception = new Exception("abc");
        RuntimeException runtimeException = new RuntimeException("def", exception);
        System.out.println("error\n" + ExceptionUtil.stacktraceToString(runtimeException));
        log.error("error", runtimeException);
    }
}
