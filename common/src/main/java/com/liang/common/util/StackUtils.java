package com.liang.common.util;

import lombok.experimental.UtilityClass;

@UtilityClass
public class StackUtils {
    public static StackTraceElement getMainFrame() {
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        // 栈底元素
        return stackTrace[stackTrace.length - 1];
    }
}
