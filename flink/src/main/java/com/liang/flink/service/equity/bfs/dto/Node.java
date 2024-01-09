package com.liang.flink.service.equity.bfs.dto;

import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.io.Serializable;

@Data
@RequiredArgsConstructor
public class Node implements Serializable {
    private final String id;
    private final String name;
}
