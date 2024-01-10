package com.liang.flink.service.equity.bfs.dto;

import lombok.Data;
import scala.collection.mutable.StringBuilder;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.math.RoundingMode.DOWN;

@Data
public class RatioPathCompanyDto {
    private final List<Chain> chains = new ArrayList<>();
    private final String shareholderId;
    private final String shareholderName;
    private final String shareholderNameId;
    private BigDecimal totalValidRatio = new BigDecimal("0");
    private boolean directShareholder = false;
    private BigDecimal directRatio = new BigDecimal("0");

    public String toDebugString() {
        StringBuilder builder = new StringBuilder(String.format("\n[%s]%s(%s)", totalValidRatio.setScale(12, DOWN).toPlainString(), shareholderName, shareholderId));
        for (Chain chain : chains) {
            builder.append("\n").append(chain.toDebugString());
        }
        return builder.toString();
    }

    public Map<String, Object> toColumnMap() {
        Map<String, Object> columnMap = new HashMap<>();
        columnMap.put("company_id", chains.get(0).getFirst().getId());
        columnMap.put("company_name", chains.get(0).getFirst().getName());
        columnMap.put("shareholder_id", shareholderId);
        columnMap.put("shareholder_name", shareholderName);
        columnMap.put("shareholder_name_id", shareholderNameId);
        columnMap.put("shareholder_entity_type", shareholderId.length() == 17 ? 2 : 1);
        columnMap.put("investment_ratio_total", totalValidRatio.setScale(12, DOWN).toPlainString());
        return columnMap;
    }
}
