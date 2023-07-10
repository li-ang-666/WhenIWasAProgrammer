package com.liang.flink.basic.new_source;

import com.liang.flink.dto.SingleCanalBinlog;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;

public class RepairSource2 implements Source<SingleCanalBinlog, RepairSourceSplit, byte[]> {
    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SplitEnumerator<RepairSourceSplit, byte[]> createEnumerator(SplitEnumeratorContext<RepairSourceSplit> enumContext) throws Exception {
        return null;
    }

    @Override
    public SplitEnumerator<RepairSourceSplit, byte[]> restoreEnumerator(SplitEnumeratorContext<RepairSourceSplit> enumContext, byte[] checkpoint) throws Exception {
        return createEnumerator(enumContext);
    }

    @Override
    public SimpleVersionedSerializer<RepairSourceSplit> getSplitSerializer() {
        return null;
    }

    @Override
    public SimpleVersionedSerializer<byte[]> getEnumeratorCheckpointSerializer() {
        return null;
    }

    @Override
    public SourceReader<SingleCanalBinlog, RepairSourceSplit> createReader(SourceReaderContext readerContext) throws Exception {
        return null;
    }
}
