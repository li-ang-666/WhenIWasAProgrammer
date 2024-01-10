package com.liang.flink.service.equity.bfs.dto;

import lombok.Data;

import java.io.Serializable;
import java.math.BigDecimal;

@Data
public class Edge implements Serializable {
    private final BigDecimal ratio;
    private final boolean isDottedLine;
}
