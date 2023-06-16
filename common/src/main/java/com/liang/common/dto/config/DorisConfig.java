package com.liang.common.dto.config;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class DorisConfig implements Serializable {
    private List<String> feHosts;
    private int port = 8030;
    private String database;
    private String user;
    private String password;
}
