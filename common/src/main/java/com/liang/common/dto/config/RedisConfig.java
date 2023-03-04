package com.liang.common.dto.config;

import lombok.Data;

import java.io.Serializable;

@Data
public class RedisConfig implements Serializable {
    private String host;
    private int port = 6379;
    private String password;
}
