package com.liang.flink.service.equity.bfs.dto.mysql;

import com.liang.flink.service.equity.bfs.dto.pojo.Path;
import lombok.Data;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static java.math.BigDecimal.ZERO;

@Data
public class RatioPathCompanyDto {
    private final List<Path> paths = new ArrayList<>();
    private final String shareholderId;
    private final String shareholderName;
    private final String shareholderNameId;
    private BigDecimal totalValidRatio = ZERO;
    private boolean isDirectShareholder = false;
    private BigDecimal directRatio = ZERO;
    private boolean isEnd = false;
}
