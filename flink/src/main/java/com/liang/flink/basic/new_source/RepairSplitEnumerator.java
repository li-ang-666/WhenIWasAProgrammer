package com.liang.flink.basic.new_source;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.List;

public class RepairSplitEnumerator implements SplitEnumerator<RepairSourceSplit, byte[]> {
    @Override
    public void start() {

    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {

    }

    @Override
    public void addSplitsBack(List<RepairSourceSplit> splits, int subtaskId) {

    }

    @Override
    public void addReader(int subtaskId) {

    }

    @Override
    public byte[] snapshotState(long checkpointId) throws Exception {
        return new byte[0];
    }

    @Override
    public void close() throws IOException {

    }
}
