package com.liang.flink.basic.repair;

import com.liang.common.dto.Config;
import com.liang.common.dto.config.RepairTask;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.service.database.template.RedisTemplate;
import com.liang.common.util.ConfigUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
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
@SuppressWarnings("deprecation")
public class RepairSource extends RichSourceFunction<RepairSplit> implements CheckpointedFunction, CheckpointListener {
    private static final int EVENLY_THRESHOLD = 1_000;
    private static final int BATCH_SIZE = 1_000;
    private static final ListStateDescriptor<RepairState> LIST_STATE_DESCRIPTOR = new ListStateDescriptor<>(RepairState.class.getSimpleName(), RepairState.class);
    private final Config config;
    private final String repairReportKey;
    private final List<RepairTask> repairTasks;
    private RepairState repairState;
    private ListState<RepairState> repairStateHolder;

    @Override
    public void initializeState(FunctionInitializationContext context) {
        try {
            ConfigUtils.setConfig(config);
            // 初始化
            repairState = new RepairState(repairTasks);
            report(String.format("init successfully, states: %s", repairState.toReportString()));
            // 恢复
            repairStateHolder = context.getOperatorStateStore().getListState(LIST_STATE_DESCRIPTOR);
            if (context.isRestored()) {
                repairStateHolder.get().forEach(repairState::restoreState);
                report(String.format("restored successfully, states: %s", repairState.toReportString()));
            }
        } catch (Exception e) {
            log.error("RepairSource initializeState failed", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void run(SourceContext<RepairSplit> ctx) {
        final Object checkpointLock = ctx.getCheckpointLock();
        for (RepairTask repairTask : repairTasks) {
            // 获取全部id
            Roaring64Bitmap allIdBitmap;
            synchronized (checkpointLock) {
                allIdBitmap = repairState.getAllIdBitmap(repairTask);
                if (allIdBitmap.isEmpty()) {
                    allIdBitmap.or(newAllIdBitmap(repairTask));
                }
            }
            // 遍历
            Roaring64Bitmap partIdBitmap = new Roaring64Bitmap();
            allIdBitmap.forEach(id -> {
                if (id > repairState.getPosition(repairTask)) {
                    partIdBitmap.add(id);
                    if (partIdBitmap.getLongCardinality() >= BATCH_SIZE) {
                        synchronized (checkpointLock) {
                            ctx.collect(new RepairSplit(repairTask, partIdBitmap));
                            repairState.updateState(repairTask, allIdBitmap, id);
                            partIdBitmap.clear();
                        }
                    }
                }
            });
            // 清空缓存
            if (!partIdBitmap.isEmpty()) {
                synchronized (checkpointLock) {
                    ctx.collect(new RepairSplit(repairTask, partIdBitmap));
                    repairState.updateState(repairTask, allIdBitmap, partIdBitmap.last());
                    partIdBitmap.clear();
                }
            }
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        repairStateHolder.clear();
        repairStateHolder.add(repairState);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        String logs = String.format("ckp_%04d successfully, states: %s",
                checkpointId,
                repairState.toReportString());
        report(logs);
    }

    @Override
    public void cancel() {
        System.exit(100);
    }

    private Roaring64Bitmap newAllIdBitmap(RepairTask repairTask) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(repairTask.getSourceName());
        // 边界
        String queryBoundSql = String.format("SELECT MIN(id), MAX(id) FROM %s", repairTask.getTableName());
        Tuple2<Long, Long> minAndMax = jdbcTemplate.queryForObject(queryBoundSql, rs -> Tuple2.of(rs.getLong(1), rs.getLong(2)));
        Long min = minAndMax.f0;
        Long max = minAndMax.f1;
        report(String.format("source: %s, table: %s, min: %,d, max: %,d", repairTask.getSourceName(), repairTask.getTableName(), min, max));
        // 粗略行数
        String queryStatusSql = String.format("SHOW TABLE STATUS LIKE '%s'", repairTask.getTableName());
        Long probablyRows = jdbcTemplate.queryForObject(queryStatusSql, rs -> rs.getLong(5));
        report(String.format("probably rows: %,d", probablyRows));
        // 偏差
        long mismatch = (max - min) / (probablyRows);
        report(String.format("mismatch: %,d", mismatch));
        // 生成
        Roaring64Bitmap bitmap;
        long start = System.currentTimeMillis();
        if (mismatch <= EVENLY_THRESHOLD) {
            report("switch to evenly, please waiting for generate id bitmap");
            bitmap = getEvenlyBitmap(min, max);
        } else {
            report("switch to unevenly, please waiting for generate id bitmap");
            bitmap = getUnevenlyBitmap(repairTask);
        }
        long end = System.currentTimeMillis();
        report(String.format("used %s seconds", (end - start) / 1000));
        return bitmap;
    }

    private Roaring64Bitmap getEvenlyBitmap(long min, long max) {
        Roaring64Bitmap bitmap = new Roaring64Bitmap();
        for (long i = min; i <= max; i++) {
            bitmap.add(i);
        }
        return bitmap;
    }

    private Roaring64Bitmap getUnevenlyBitmap(RepairTask repairTask) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(repairTask.getSourceName());
        String sql = String.format("SELECT id FROM %s", repairTask.getTableName());
        Roaring64Bitmap bitmap = new Roaring64Bitmap();
        jdbcTemplate.streamQuery(true, sql, rs -> bitmap.add(rs.getLong(1)));
        return bitmap;
    }

    private void report(String logs) {
        logs = String.format("[_RepairSource_] %s", logs);
        new RedisTemplate("metadata").rPush(repairReportKey, logs);
    }
}
