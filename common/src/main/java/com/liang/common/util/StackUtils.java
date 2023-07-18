package com.liang.common.util;

import lombok.experimental.UtilityClass;

@UtilityClass
public class StackUtils {
    public static StackTraceElement getMainFrame() {
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        for (StackTraceElement traceElement : stackTrace) {
            if ("main".equals(traceElement.getMethodName())) {
                return traceElement;
            }
        }
        StackTraceElement latest = stackTrace[stackTrace.length - 1];
        throw new RuntimeException(String.format("there is no main method, trace start from %s", latest));
    }
}
