package com.liang.common.dto.config;

import lombok.Data;

import java.io.Serializable;

@Data
public class FlinkConfig implements Serializable {
    private SourceType sourceType;
    private int sourceParallel;
    private int otherParallel;

    public enum SourceType {
        KAFKA, REPAIR
    }
}
