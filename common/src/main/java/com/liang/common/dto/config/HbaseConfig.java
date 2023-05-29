package com.liang.common.dto.config;

import lombok.Data;

import java.io.Serializable;

@Data
public class HbaseConfig implements Serializable {
    private String zookeeperQuorum;
}
