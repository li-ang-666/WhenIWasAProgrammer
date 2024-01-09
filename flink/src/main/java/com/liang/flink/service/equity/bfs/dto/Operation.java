package com.liang.flink.service.equity.bfs.dto;

public enum Operation {
    // 更新链路❌ 更新比例❌
    DROP,

    // 更新链路✅ 更新比例❌
    UPDATE_CHAIN_ONLY,

    // 更新链路✅ 更新比例✅
    UPDATE_CHAIN_AND_RATIO,

    // 更新链路✅ 更新比例✅(不归档)
    NOT_ARCHIVE
}
