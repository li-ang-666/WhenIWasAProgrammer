package com.liang.flink.service.equity.bfs.dto;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class RatioPathCompanyDto {
    private final List<Chain> chains = new ArrayList<>();
    private final String shareholderId;
    private final String shareholderName;
}
