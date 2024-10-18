package com.liang.flink.project.equity.bfs.dto;

public enum Operation {
    /**
     * 更新链路❌ 更新比例❌(归档)
     */
    DROP,

    /**
     * 更新链路✅ 更新比例❌(归档)
     */
    ARCHIVE_WITH_UPDATE_PATH_ONLY,

    /**
     * 更新链路✅ 更新比例✅(归档)
     */
    ARCHIVE_WITH_UPDATE_PATH_AND_RATIO,

    /**
     * 更新链路✅ 更新比例✅(不归档)
     */
    NOT_ARCHIVE
}
