package com.liang.common.dto;

import com.liang.common.service.Lists;
import lombok.Data;

@Data
public class StreamLoadDto {
    private String host;
    private int httpPort;
    private String user;
    private String pwd;
    private String database;
    private String table;
    private Lists<String> schema;
    private String data;
}
