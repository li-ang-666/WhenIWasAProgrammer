package com.liang.flink.service.equity.bfs.dto.mysql;

import com.liang.flink.service.equity.bfs.dto.pojo.Path;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static java.math.BigDecimal.ZERO;

@Data
@RequiredArgsConstructor
public class RatioPathCompanyDto {
    private final String shareholderId;
    private final String shareholderName;
    private final String shareholderNameId;
    private List<Path> paths = new ArrayList<>();
    private BigDecimal totalValidRatio = ZERO;
    private boolean isDirectShareholder = false;
    private BigDecimal directRatio = ZERO;
    private boolean isEnd = false;
}
