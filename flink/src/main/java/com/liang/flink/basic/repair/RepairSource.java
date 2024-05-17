package com.liang.flink.basic.repair;

import cn.hutool.core.util.SerializeUtil;
import com.liang.common.dto.Config;
import com.liang.common.dto.config.RepairTask;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.service.database.template.RedisTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import com.liang.common.util.SqlUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
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
    private static final int MIN_QUERY_BATCH_SIZE = 1024;
    private static final int MAX_QUERY_BATCH_SIZE = 10240;
    private static final int DIRECT_SCAN_COMPLETE_FLAG = -1;
    private static final int SAMPLING_INTERVAL_TIMES = 10;
    // flink web ui cancel
    private final AtomicBoolean canceled = new AtomicBoolean(false);
    private final Config config;
    private final String repairKey;
    private int currentQueryBatchSize = MIN_QUERY_BATCH_SIZE;
    private RepairTask task;
    private ListState<RepairTask> taskState;
    private RedisTemplate redisTemplate;
    private String baseDetectSql;
    private String baseSplitSql;
    private JdbcTemplate jdbcTemplate;

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
        baseSplitSql = new SQL()
                .SELECT(task.getColumns())
                .FROM(task.getTableName())
                .WHERE(task.getWhere())
                .toString();
        baseDetectSql = new SQL()
                .SELECT("count(1)")
                .SELECT(String.format("count(if(%s, 1, null))", task.getWhere()))
                .FROM(task.getTableName())
                .toString();
        redisTemplate = new RedisTemplate("metadata");
        jdbcTemplate = new JdbcTemplate(task.getSourceName());
    }

    @Override
    public void run(SourceContext<RepairSplit> ctx) {
        int sendTimes = 0;
        while (!canceled.get() && hasNext()) {
            synchronized (ctx.getCheckpointLock()) {
                int channel = task.getChannels().get(sendTimes % task.getChannels().size());
                ctx.collect(new RepairSplit(task.getTaskId(), task.getSourceName(), task.getTableName(), channel, nextSql()));
                if (++sendTimes % SAMPLING_INTERVAL_TIMES == 0) {
                    Tuple2<Integer, Integer> rowsTuple2 = detectSplitRows();
                    int rowsWithoutWhere = rowsTuple2.f0;
                    int rowsWithWhere = rowsTuple2.f1;
                    // 空区间, 采用jdbc矫正
                    if (rowsWithoutWhere == 0 && currentQueryBatchSize == MAX_QUERY_BATCH_SIZE) {
                        commit(true);
                        log.info("pivot corrected to {} by jdbc", task.getPivot());
                    }
                    // 稀疏区间, 适当加大batch
                    else if (rowsWithWhere <= MIN_QUERY_BATCH_SIZE * 0.8) {
                        commit(false);
                        currentQueryBatchSize = Math.min(currentQueryBatchSize * 2, MAX_QUERY_BATCH_SIZE);
                        log.info("query batch size upgraded to {}", currentQueryBatchSize);
                    }
                    // 非稀疏区间, 适当降低batch
                    else if (rowsWithWhere > MIN_QUERY_BATCH_SIZE * 1.8) {
                        commit(false);
                        currentQueryBatchSize = Math.max(currentQueryBatchSize / 2, MIN_QUERY_BATCH_SIZE);
                        log.info("query batch size downgraded to {}", currentQueryBatchSize);
                    } else {
                        commit(false);
                    }
                } else {
                    commit(false);
                }
            }
        }
        reportComplete();
        checkFinish();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        RepairTask copyTask = SerializeUtil.clone(task);
        taskState.clear();
        taskState.add(copyTask);
        reportCheckpoint(copyTask);
    }

    private boolean hasNext() {
        return task.getScanMode() == Direct ?
                task.getPivot() != DIRECT_SCAN_COMPLETE_FLAG : task.getPivot() < task.getUpperBound();
    }

    private String nextSql() {
        StringBuilder sqlBuilder = new StringBuilder(baseSplitSql);
        if (task.getScanMode() == TumblingWindow) {
            sqlBuilder.append(String.format(" AND %s <= id AND id < %s", task.getPivot(), nextPivot()));
        }
        return sqlBuilder.toString();
    }

    private Tuple2<Integer, Integer> detectSplitRows() {
        String sql = baseDetectSql +
                String.format(" WHERE %s <= id AND id < %s", task.getPivot(), nextPivot());
        return jdbcTemplate.queryForObject(sql,
                rs -> Tuple2.of(rs.getInt(1), rs.getInt(2)));
    }

    private void commit(boolean useJdbc) {
        long queriedPivot = task.getScanMode() == Direct ?
                DIRECT_SCAN_COMPLETE_FLAG : nextPivot();
        task.setPivot(queriedPivot);
        if (!useJdbc) {
            return;
        }
        String sql = new SQL()
                .SELECT("min(id)")
                .FROM(task.getTableName())
                .WHERE("id >= " + SqlUtils.formatValue(task.getPivot()))
                .toString();
        Long nextPivot = jdbcTemplate.queryForObject(sql, rs -> rs.getLong(1));
        if (nextPivot == null) {
            task.setPivot(task.getUpperBound());
        } else {
            task.setPivot(Math.min(nextPivot, task.getUpperBound()));
        }
    }

    private long nextPivot() {
        return Math.min(task.getPivot() + currentQueryBatchSize, task.getUpperBound());
    }

    private void reportCheckpoint(RepairTask copyTask) {
        if (hasNext()) {
            Long pivot = copyTask.getPivot();
            Long upperBound = copyTask.getUpperBound();
            String info = String.format("%s table: %s, pivot: %,d, upperBound: %,d, lag: %,d",
                    RUNNING_REPORT_PREFIX, copyTask.getTableName(), pivot, upperBound, upperBound - pivot);
            redisTemplate.hSet(repairKey, String.format("%03d", copyTask.getTaskId()), info);
        }
    }

    private void reportComplete() {
        String info = String.format("%s table: %s, final pivot: %,d",
                COMPLETE_REPORT_PREFIX, task.getTableName(), task.getPivot());
        redisTemplate.hSet(repairKey, String.format("%03d", task.getTaskId()), info);
    }

    private void checkFinish() {
        while (!canceled.get()) {
            LockSupport.parkUntil(System.currentTimeMillis() + CHECK_COMPLETE_INTERVAL_MILLISECONDS);
            Map<String, String> repairMap = redisTemplate.hScan(repairKey);
            long numCompleted = repairMap.values().stream().filter(e -> e.startsWith(COMPLETE_REPORT_PREFIX)).count();
            if (numCompleted != config.getRepairTasks().size()) {
                continue;
            }
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
