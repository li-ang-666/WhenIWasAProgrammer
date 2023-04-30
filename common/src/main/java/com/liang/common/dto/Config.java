package com.liang.common.dto;


import com.liang.common.dto.config.DBConfig;
import com.liang.common.dto.config.RedisConfig;
import lombok.Data;

import java.io.Serializable;
import java.util.Map;

@Data
public class Config implements Serializable {
    private Map<String, DBConfig> DBConfigs;
    private Map<String, RedisConfig> redisConfigs;
}

