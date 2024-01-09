package com.liang.flink.service.equity.bfs.dto;

import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

@Data
public class RatioPathCompanyDto {
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private final List<Chain> chains = new ArrayList<>();
    private final String shareholderId;
    private final String shareholderName;
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private BigDecimal totalValidRatio = new BigDecimal("0");
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private boolean directShareholder = false;
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private BigDecimal directRatio = new BigDecimal("0");

    public void addChain(Chain chain) {
        chains.add(chain);
        totalValidRatio = totalValidRatio.add(chain.getValidRatio());
    }

    public void registerDirectShareholder() {
        if (directShareholder) return;
        directShareholder = true;
        directRatio = new BigDecimal(totalValidRatio.toPlainString());
    }
}
