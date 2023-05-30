package com.liang.common.dto.config;

import lombok.Data;

import java.io.Serializable;

@Data
public class HbaseDbConfig implements Serializable {
    private String zookeeperQuorum;
}
