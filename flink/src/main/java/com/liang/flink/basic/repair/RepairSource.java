package com.liang.flink.basic.repair;

import cn.hutool.cron.task.Task;
import com.liang.common.dto.Config;
import com.liang.common.dto.config.RepairTask;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.service.database.template.RedisTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.util.List;

/*
 * https://nightlies.apache.org/flink/flink-docs-release-1.17/zh/docs/dev/datastream/fault-tolerance/checkpointing
 * 部分任务结束后的 Checkpoint
 */
@Slf4j
@RequiredArgsConstructor
public class RepairSource extends RichSourceFunction<RepairSplit> implements CheckpointedFunction {
    private static final int BATCH_SIZE = 1_000;
    private static final ListStateDescriptor<RepairState> LIST_STATE_DESCRIPTOR = new ListStateDescriptor<>(RepairState.class.getSimpleName(), RepairState.class);
    private final Config config;
    private final String repairReportKey;
    private final List<RepairTask> repairTasks;
    private RedisTemplate redisTemplate;
    private RepairState repairState;
    private ListState<RepairState> repairStateHolder;

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ConfigUtils.setConfig(config);
        redisTemplate = new RedisTemplate("metadata");
        // 初始化
        repairState = new RepairState(repairTasks);
        reportAndLog(String.format("init successfully, states: %s", JsonUtils.toString(repairState.getStates())));
        // 恢复
        repairStateHolder = context.getOperatorStateStore().getListState(LIST_STATE_DESCRIPTOR);
        if (context.isRestored()) {
            for (RepairState restoredState : repairStateHolder.get()) {
                repairState.initializeState(restoredState);
            }
            reportAndLog(String.format("restored successfully, states: %s", JsonUtils.toString(repairState.getStates())));
        }
    }

    @Override
    public void run(SourceContext<RepairSplit> ctx) {
        final Object checkpointLock = ctx.getCheckpointLock();
        for (RepairTask repairTask : repairTasks) {
            // 初始化
            JdbcTemplate jdbcTemplate = new JdbcTemplate(repairTask.getSourceName());
            SQL sql = new SQL().SELECT("id")
                    .FROM(repairTask.getTableName())
                    .ORDER_BY("id ASC");
            // 单并发时,只查符合指定where的id
            if (config.getFlinkConfig().getSourceParallel() == 1) {
                sql.WHERE(repairTask.getWhere());
            }
            Roaring64Bitmap cachedIds = new Roaring64Bitmap();
            Task snapshotSplit = () -> {
                ctx.collect(new RepairSplit(repairTask, cachedIds.first(), cachedIds.last()));
                repairState.snapshotState(repairTask, cachedIds);
                cachedIds.clear();
            };
            // 执行
            jdbcTemplate.streamQuery(true, sql.toString(), rs -> {
                synchronized (checkpointLock) {
                    cachedIds.add(rs.getLong("id"));
                    if (cachedIds.getLongCardinality() >= BATCH_SIZE) {
                        snapshotSplit.execute();
                    }
                }
            });
            // 扫尾
            synchronized (checkpointLock) {
                if (!cachedIds.isEmpty()) {
                    snapshotSplit.execute();
                }
            }
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        repairStateHolder.clear();
        repairStateHolder.add(repairState);
        String logs = String.format("ckp_%d successfully, states: %s",
                context.getCheckpointId(),
                JsonUtils.toString(repairState.getStates())
        );
        reportAndLog(logs);
    }

    @Override
    public void cancel() {
        System.exit(1);
    }

    private void reportAndLog(String logs) {
        logs = String.format("[RepairSource] %s", logs);
        redisTemplate.rPush(repairReportKey, logs);
        log.info("{}", logs);
    }
}
