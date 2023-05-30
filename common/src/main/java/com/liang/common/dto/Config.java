package com.liang.common.dto;


import com.liang.common.dto.config.*;
import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
public class Config implements Serializable {
    private FlinkSource flinkSource;
    private List<RepairTask> repairTasks;
    private Map<String, DBConfig> dbConfigs;
    private Map<String, RedisConfig> redisConfigs;
    private Map<String, KafkaConfig> kafkaConfigs;
    private Map<String, HbaseDbConfig> hbaseDbConfigs;
    private Map<String, HbaseSchema> hbaseSchemas;
}

