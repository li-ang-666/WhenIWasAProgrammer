package com.liang.common.dto;


import com.liang.common.dto.config.DBConfig;
import com.liang.common.dto.config.HbaseConfig;
import com.liang.common.dto.config.KafkaConfig;
import com.liang.common.dto.config.RedisConfig;
import lombok.Data;

import java.io.Serializable;
import java.util.Map;

@Data
public class Config implements Serializable {
    private Map<String, DBConfig> dbConfigs;
    private Map<String, RedisConfig> redisConfigs;
    private Map<String, KafkaConfig> kafkaConfigs;
    private Map<String, HbaseConfig> hbaseConfigs;
}

