package com.liang.flink.project.equity.bfs.dto.mysql;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class CompanyEquityRelationDetailsDto {
    private final String shareholderId;
    private final String shareholderName;
    private final BigDecimal ratio;
}
