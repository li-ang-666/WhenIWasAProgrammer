package com.liang.flink.basic.repair;

import com.liang.common.dto.Config;
import com.liang.common.dto.config.RepairTask;
import com.liang.common.util.ConfigUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/*
 * https://nightlies.apache.org/flink/flink-docs-release-1.17/zh/docs/dev/datastream/fault-tolerance/checkpointing
 * 部分任务结束后的 Checkpoint
 */
@Slf4j
@RequiredArgsConstructor
public class RepairSource extends RichParallelSourceFunction<RepairSplit> {
    private static final int QUERY_BATCH_SIZE = 100_000_000;
    private final AtomicBoolean canceled = new AtomicBoolean(false);
    private final Config config;

    @Override
    public void open(Configuration parameters) {
        ConfigUtils.setConfig(config);
        List<RepairTask> repairTasks = config.getRepairTasks();
        for (RepairTask repairTask : repairTasks) {

        }
    }

    @Override
    public void run(SourceContext<RepairSplit> ctx) {
        while (!canceled.get() && hasNextSplit()) {
            for (Integer channel : task.getChannels()) {
                if (hasNextSplit()) {
                    synchronized (ctx.getCheckpointLock()) {
                        ctx.collect(nextSplit(channel));
                        commit();
                    }
                }
            }
        }
        cancel();
    }

    @Override
    public void cancel() {
        canceled.set(true);
    }
}
