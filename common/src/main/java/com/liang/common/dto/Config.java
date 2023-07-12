package com.liang.common.dto;


import com.liang.common.dto.config.*;
import lombok.Data;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class Config implements Serializable {
    private FlinkConfig flinkConfig;
    private List<RepairTask> repairTasks;
    // 常用库
    private Map<String, DBConfig> dbConfigs = new HashMap<>();
    private Map<String, RedisConfig> redisConfigs = new HashMap<>();
    // kafka
    private Map<String, KafkaConfig> kafkaConfigs = new HashMap<>();
    // 特殊库
    private Map<String, HbaseDbConfig> hbaseDbConfigs = new HashMap<>();
    private Map<String, DorisDbConfig> dorisDbConfigs = new HashMap<>();
}

