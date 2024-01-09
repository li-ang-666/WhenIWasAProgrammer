package com.liang.flink.service.equity.bfs.dto;

import lombok.Data;

import java.io.Serializable;

@Data
public class Node implements Serializable {
    private final String id;
    private final String name;
}
