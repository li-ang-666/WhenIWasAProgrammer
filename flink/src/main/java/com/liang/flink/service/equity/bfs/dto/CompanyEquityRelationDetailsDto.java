package com.liang.flink.service.equity.bfs.dto;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class CompanyEquityRelationDetailsDto {
    private final String shareholderId;
    private final String shareholderName;
    private final BigDecimal ratio;
}
