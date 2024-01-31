package com.liang.flink.service.equity.bfs.dto.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.math.BigDecimal;

@Data
@AllArgsConstructor
public class Edge implements PathElement, Serializable {
    private BigDecimal ratio;
    private boolean isValid;
}
