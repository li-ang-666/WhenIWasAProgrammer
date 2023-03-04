package com.liang.common.dto.config;

import lombok.Data;

import java.io.Serializable;
import java.util.Map;

@Data
public class ConnectionConfig implements Serializable {
    private Map<String, MySQLConfig> mysqlConfigs;
    private Map<String, RedisConfig> redisConfigs;
}
