package com.liang.common.dto.config;

import lombok.Data;

import java.io.Serializable;

@Data
public class DBConfig implements Serializable {
    private String host;
    private int port = 3306;
    private String database;
    private String user = "jdhw_d_data_dml";
    private String password = "2s0^tFa4SLrp72";
}
