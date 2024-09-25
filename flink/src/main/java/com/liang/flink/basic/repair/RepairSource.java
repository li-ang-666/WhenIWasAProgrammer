package com.liang.flink.basic.repair;

import com.liang.common.dto.Config;
import com.liang.common.dto.config.RepairTask;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.service.database.template.RedisTemplate;
import com.liang.common.util.ConfigUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.util.List;

/*
 * https://nightlies.apache.org/flink/flink-docs-release-1.17/zh/docs/dev/datastream/fault-tolerance/checkpointing
 * 部分任务结束后的 Checkpoint
 */
@Slf4j
@RequiredArgsConstructor
public class RepairSource extends RichParallelSourceFunction<RepairSplit> implements CheckpointedFunction {
    private static final long BATCH_SIZE = 1000;
    private final Config config;
    private final String repairReportKey;
    private final List<RepairTask> repairTasks;
    private RedisTemplate redisTemplate;

    @Override
    public void initializeState(FunctionInitializationContext context) {
        ConfigUtils.setConfig(config);
        redisTemplate = new RedisTemplate("metadata");
    }

    @Override
    public void run(SourceContext<RepairSplit> ctx) {
        final Object checkpointLock = ctx.getCheckpointLock();
        for (RepairTask repairTask : repairTasks) {
            // 初始化
            JdbcTemplate jdbcTemplate = new JdbcTemplate(repairTask.getSourceName());
            String sql = new SQL().SELECT("id")
                    .FROM(repairTask.getTableName())
                    .ORDER_BY("id")
                    .toString();
            Roaring64Bitmap ids = new Roaring64Bitmap();
            // 执行
            jdbcTemplate.streamQuery(true, sql, rs -> {
                synchronized (checkpointLock) {
                    long id = rs.getLong("id");
                    ids.add(id);
                    if (ids.getLongCardinality() >= BATCH_SIZE) {
                        ctx.collect(new RepairSplit(repairTask, ids));
                        ids.clear();
                    }
                }
            });
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) {
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
