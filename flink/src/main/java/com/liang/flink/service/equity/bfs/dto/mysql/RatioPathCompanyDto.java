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
    // 被投资公司基本属性
    private final String companyId;
    private final String companyName;
    // 股东基本属性
    private final String shareholderType;
    private final String shareholderId;
    private final String shareholderName;
    private final String shareholderNameId;
    private final String shareholderMasterCompanyId;
    // 路径明细 & 比例
    private List<Path> paths = new ArrayList<>();
    private BigDecimal totalValidRatio = ZERO;
    // 直接关系
    private boolean isDirectShareholder = false;
    private BigDecimal directRatio = ZERO;
    // 是否穿透终点
    private boolean isEnd = false;
}
