package com.liang.flink.project.equity.bfs.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ShareholderJudgeInfo {
    private final String companyId;
    private final boolean isClosed;
    private final boolean is001;
}
