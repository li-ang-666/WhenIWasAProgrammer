package com.liang.flink.project.equity.bfs.dto.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class Node implements PathElement, Serializable {
    private String id;
    private String name;
}
