package com.liang.common.service;

import com.liang.common.service.Timer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Logging {
    private final Timer timer = new Timer();
    private final String errorLog;
    private final String afterLog;

    public Logging(String classShortName, String instanceName) {
        String commonLog = String.format("[%s %s].", classShortName, instanceName);
        errorLog = commonLog + "{}({}) error";
        afterLog = commonLog + "{}({}) {}ms";
    }

    public void beforeExecute() {
        timer.remake();
    }

    public void ifError(String methodName, Object methodArg, Exception e) {
        log.error(errorLog, methodName, methodArg, e);
    }

    public void afterExecute(String methodName, Object methodArg) {
        long interval = timer.getInterval();
        if (interval > 1000L) {
            log.warn(afterLog, methodName, methodArg, interval);
        } else if (log.isDebugEnabled()) {
            log.debug(afterLog, methodName, methodArg, interval);
        }
    }
}
