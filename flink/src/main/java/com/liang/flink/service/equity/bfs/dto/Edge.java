package com.liang.flink.service.equity.bfs.dto;

import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.io.Serializable;
import java.math.BigDecimal;

@Data
@RequiredArgsConstructor
public class Edge implements Serializable {
    private final BigDecimal ratio;
    private final boolean dottedLine;
}
