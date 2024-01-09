package com.liang.flink.service.equity.bfs.dto;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;

@Data
@Builder
public class CompanyEquityRelationDetailsDto {
    private final String shareholderId;
    private final String shareholderName;
    private final BigDecimal ratio;
}
