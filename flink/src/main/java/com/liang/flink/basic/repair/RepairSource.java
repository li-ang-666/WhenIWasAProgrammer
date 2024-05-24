package com.liang.flink.basic.repair;

import com.liang.common.dto.Config;
import com.liang.common.dto.config.RepairTask;
import com.liang.common.service.SQL;
import com.liang.common.util.ConfigUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.concurrent.atomic.AtomicBoolean;

import static com.liang.common.dto.config.RepairTask.ScanMode.Direct;
import static com.liang.common.dto.config.RepairTask.ScanMode.TumblingWindow;

/*
 * https://nightlies.apache.org/flink/flink-docs-release-1.17/zh/docs/dev/datastream/fault-tolerance/checkpointing
 * 部分任务结束后的 Checkpoint
 */
@Slf4j
@RequiredArgsConstructor
public class RepairSource extends RichParallelSourceFunction<RepairSplit> {
    private static final int QUERY_BATCH_SIZE = 1_000_000;
    private static final int DIRECT_SCAN_COMPLETE_FLAG = -1;
    private final AtomicBoolean canceled = new AtomicBoolean(false);
    private final Config config;
    private RepairTask task;
    private String baseSplitSql;

    @Override
    public void open(Configuration parameters) {
        ConfigUtils.setConfig(config);
        task = config.getRepairTasks().get(getRuntimeContext().getIndexOfThisSubtask());
        baseSplitSql = new SQL()
                .SELECT(task.getColumns())
                .FROM(task.getTableName())
                .WHERE(task.getWhere())
                .toString();
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

    private boolean hasNextSplit() {
        return task.getScanMode() == Direct ?
                task.getPivot() != DIRECT_SCAN_COMPLETE_FLAG : task.getPivot() < task.getUpperBound();
    }

    private RepairSplit nextSplit(int channel) {
        String sql = baseSplitSql + (task.getScanMode() == TumblingWindow ? String.format(" AND %s <= id AND id < %s", task.getPivot(), nextPivot()) : "");
        return new RepairSplit(task.getTaskId(), task.getSourceName(), task.getTableName(), channel, sql);
    }

    // 写pivot
    private void commit() {
        long nextPivot = task.getScanMode() == Direct ? DIRECT_SCAN_COMPLETE_FLAG : nextPivot();
        task.setPivot(nextPivot);
    }

    private long nextPivot() {
        return Math.min(task.getPivot() + QUERY_BATCH_SIZE, task.getUpperBound());
    }

    @Override
    public void cancel() {
        canceled.set(true);
    }
}
