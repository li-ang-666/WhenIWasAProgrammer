package com.liang.common.dto.config;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class DorisDbConfig implements Serializable {
    private List<String> fe;
    private String user;
    private String password;
}
