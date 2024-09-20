package com.liang.flink.basic.repair;

import com.liang.common.dto.Config;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.dto.SingleCanalBinlog;
import lombok.RequiredArgsConstructor;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Collector;

@RequiredArgsConstructor
public class RepairHandler extends RichFlatMapFunction<SingleCanalBinlog, SingleCanalBinlog> implements CheckpointedFunction {
    private final Config config;


    @Override
    public void initializeState(FunctionInitializationContext context) {
        ConfigUtils.setConfig(config);
        context.getOperatorStateStore();
    }

    @Override
    public void flatMap(SingleCanalBinlog singleCanalBinlog, Collector<SingleCanalBinlog> out) {
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) {

    }

}
