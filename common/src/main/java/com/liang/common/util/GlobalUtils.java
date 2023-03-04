package com.liang.common.util;

import com.liang.common.dto.config.ConnectionConfig;
import org.apache.flink.api.java.utils.ParameterTool;

public class GlobalUtils {
    private static ConnectionConfig connectionConfig;
    private static ParameterTool parameterTool;

    private GlobalUtils() {
    }

    public static void init(ConnectionConfig connectionConfig, ParameterTool parameterTool) {
        if (GlobalUtils.connectionConfig == null && connectionConfig != null) {
            synchronized (GlobalUtils.class) {
                if (GlobalUtils.connectionConfig == null)
                    GlobalUtils.connectionConfig = connectionConfig;
            }
        }
        if (GlobalUtils.parameterTool == null && parameterTool != null) {
            synchronized (GlobalUtils.class) {
                if (GlobalUtils.parameterTool == null)
                    GlobalUtils.parameterTool = parameterTool;
            }
        }
    }

    public static ConnectionConfig getConnectionConfig() {
        return GlobalUtils.connectionConfig;
    }

    public static ParameterTool getParameterTool() {
        return GlobalUtils.parameterTool;
    }
}
