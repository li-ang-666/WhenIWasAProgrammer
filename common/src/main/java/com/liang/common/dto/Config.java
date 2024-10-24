package com.liang.common.dto;


import com.liang.common.dto.config.*;
import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
public class Config implements Serializable {
    private FlinkConfig flinkConfig;
    private List<RepairTask> repairTasks;
    // 常用库
    private Map<String, DBConfig> dbConfigs;
    private Map<String, RedisConfig> redisConfigs;
    // kafka
    private Map<String, KafkaConfig> kafkaConfigs;
    // 特殊库
    private Map<String, HbaseConfig> hbaseConfigs;
    private Map<String, DorisConfig> dorisConfigs;
    // Doris
    private DorisSchema dorisSchema;
    // 其他
    private Map<String, Object> otherConfigs;
}

