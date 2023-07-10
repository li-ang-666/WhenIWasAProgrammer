package com.liang.flink.basic.new_source;

import org.apache.flink.api.connector.source.SourceSplit;

public class RepairSourceSplit implements SourceSplit {
    @Override
    public String splitId() {
        return null;
    }
}
