package com.liang.flink.basic.repair;

import cn.hutool.core.util.SerializeUtil;
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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import static com.liang.common.dto.config.RepairTask.ScanMode.Direct;
import static com.liang.common.dto.config.RepairTask.ScanMode.TumblingWindow;

/*
 * https://nightlies.apache.org/flink/flink-docs-release-1.17/zh/docs/dev/datastream/fault-tolerance/checkpointing
 * 部分任务结束后的 Checkpoint
 */
@Slf4j
@RequiredArgsConstructor
public class RepairSource extends RichParallelSourceFunction<RepairSplit> implements CheckpointedFunction {
    // state
    private static final String TASK_STATE_NAME = "TASK_STATE";
    private static final ListStateDescriptor<RepairTask> TASK_STATE_DESCRIPTOR = new ListStateDescriptor<>(TASK_STATE_NAME, RepairTask.class);
    // check and tell redis
    private static final String RUNNING_REPORT_PREFIX = "[checkpoint]";
    private static final String COMPLETE_REPORT_PREFIX = "[completed]";
    private static final int CHECK_COMPLETE_INTERVAL_MILLISECONDS = 1000 * 3;
    // query
    private static final int QUERY_BATCH_SIZE = 100_000;
    private static final int DIRECT_SCAN_COMPLETE_FLAG = -1;
    // flink web ui cancel
    private final AtomicBoolean canceled = new AtomicBoolean(false);
    private final Config config;
    private final String repairKey;
    private RepairTask task;
    private ListState<RepairTask> taskState;
    private String baseDetectSql;
    private String baseRedirectSql;
    private String baseSplitSql;
    private JdbcTemplate jdbcTemplate;
    private RedisTemplate redisTemplate;

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // 初始化task与state
        ConfigUtils.setConfig(config);
        task = config.getRepairTasks().get(getRuntimeContext().getIndexOfThisSubtask());
        // 从ckp恢复task
        taskState = context.getOperatorStateStore().getUnionListState(TASK_STATE_DESCRIPTOR);
        for (RepairTask stateTask : taskState.get()) {
            if (stateTask.getTaskId().equals(task.getTaskId()) && stateTask.getTableName().equals(task.getTableName())) {
                // 仅恢复pivot
                task.setPivot(stateTask.getPivot());
                log.info("restored from state, task-{}: {}", task.getTaskId(), JsonUtils.toString(task));
                return;
            }
        }
    }

    @Override
    public void open(Configuration parameters) {
        baseDetectSql = new SQL()
                .SELECT("if(COUNT(1) = 0, TRUE, FALSE)")
                .FROM(task.getTableName())
                .toString();
        baseRedirectSql = new SQL()
                .SELECT("MIN(id)")
                .FROM(task.getTableName())
                .toString();
        baseSplitSql = new SQL()
                .SELECT(task.getColumns())
                .FROM(task.getTableName())
                .WHERE(task.getWhere())
                .toString();
        jdbcTemplate = new JdbcTemplate(task.getSourceName());
        redisTemplate = new RedisTemplate("metadata");
    }

    @Override
    public void run(SourceContext<RepairSplit> ctx) {
        while (!canceled.get()) {
            for (Integer channel : task.getChannels()) {
                if (hasNextSplit()) {
                    synchronized (ctx.getCheckpointLock()) {
                        if (task.getScanMode() == TumblingWindow && detectBlank()) {
                            redirectPivot();
                            log.info("pivot redirected to {}", task.getPivot());
                        }
                        // pivot重定向后, 重新计算一次hasNextSplit()
                        if (hasNextSplit()) {
                            ctx.collect(nextSplit(channel));
                            commit();
                        }
                    }
                }
            }
        }
        reportComplete();
        while (!canceled.get()) {
            checkFinish();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        RepairTask copyTask = SerializeUtil.clone(task);
        taskState.clear();
        taskState.add(copyTask);
        if (hasNextSplit()) {
            reportCheckpoint(copyTask);
        }
    }

    private boolean hasNextSplit() {
        return task.getScanMode() == Direct ?
                task.getPivot() != DIRECT_SCAN_COMPLETE_FLAG : task.getPivot() < task.getUpperBound();
    }

    private RepairSplit nextSplit(int channel) {
        String sql = baseSplitSql + (task.getScanMode() == TumblingWindow ? String.format(" AND %s <= id AND id < %s", task.getPivot(), nextPivot()) : "");
        return new RepairSplit(task.getTaskId(), task.getSourceName(), task.getTableName(), channel, sql);
    }

    private boolean detectBlank() {
        String sql = baseDetectSql + String.format(" WHERE %s <= id AND id < %s", task.getPivot(), nextPivot());
        return jdbcTemplate.queryForObject(sql, rs -> rs.getBoolean(1));
    }

    // 写pivot
    private void redirectPivot() {
        String sql = baseRedirectSql + String.format(" WHERE id >= %s", task.getPivot());
        Long nextPivot = jdbcTemplate.queryForObject(sql, rs -> rs.getLong(1));
        task.setPivot(nextPivot == null ? task.getUpperBound() : Math.min(nextPivot, task.getUpperBound()));
    }

    // 写pivot
    private void commit() {
        long nextPivot = task.getScanMode() == Direct ? DIRECT_SCAN_COMPLETE_FLAG : nextPivot();
        task.setPivot(nextPivot);
    }

    private long nextPivot() {
        return Math.min(task.getPivot() + QUERY_BATCH_SIZE, task.getUpperBound());
    }

    private void reportCheckpoint(RepairTask copyTask) {
        Long pivot = copyTask.getPivot();
        Long upperBound = copyTask.getUpperBound();
        String info = String.format("%s table: %s, pivot: %,d, upperBound: %,d, lag: %,d",
                RUNNING_REPORT_PREFIX, copyTask.getTableName(), pivot, upperBound, upperBound - pivot);
        redisTemplate.hSet(repairKey, String.format("%03d", copyTask.getTaskId()), info);
    }

    private void reportComplete() {
        String info = String.format("%s table: %s, final pivot: %,d",
                COMPLETE_REPORT_PREFIX, task.getTableName(), task.getPivot());
        redisTemplate.hSet(repairKey, String.format("%03d", task.getTaskId()), info);
    }

    private void checkFinish() {
        LockSupport.parkUntil(System.currentTimeMillis() + CHECK_COMPLETE_INTERVAL_MILLISECONDS);
        Map<String, String> repairMap = redisTemplate.hScan(repairKey);
        long numCompleted = repairMap.values().stream().filter(e -> e.startsWith(COMPLETE_REPORT_PREFIX)).count();
        if (numCompleted == config.getRepairTasks().size()) {
            log.info("detected all repair task has been completed, RepairTask-{} will be cancel after the next checkpoint", task.getTaskId());
            cancel();
        }
    }

    @Override
    public void close() {
        cancel();
    }

    @Override
    public void cancel() {
        canceled.set(true);
    }
}
