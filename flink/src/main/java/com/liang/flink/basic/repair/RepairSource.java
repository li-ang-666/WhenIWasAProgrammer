package com.liang.flink.basic.repair;

import cn.hutool.core.util.SerializeUtil;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.liang.common.dto.Config;
import com.liang.common.dto.config.RepairTask;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.service.database.template.RedisTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import com.liang.common.util.SqlUtils;
import com.liang.flink.dto.SingleCanalBinlog;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.HashMap;
import java.util.List;
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
public class RepairSource extends RichParallelSourceFunction<SingleCanalBinlog> implements CheckpointedFunction {
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
    // flink web ui cancel
    private final AtomicBoolean canceled = new AtomicBoolean(false);
    private final Config config;
    private final String repairKey;
    private volatile int currentQueryBatchSize = MIN_QUERY_BATCH_SIZE;
    private RepairTask task;
    private ListState<RepairTask> taskState;
    private RedisTemplate redisTemplate;
    private String baseSql;
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
        baseSql = new SQL()
                .SELECT(task.getColumns())
                .FROM(task.getTableName())
                .WHERE(task.getWhere())
                .toString();
        redisTemplate = new RedisTemplate("metadata");
        jdbcTemplate = new JdbcTemplate(task.getSourceName());
    }

    @Override
    public void run(SourceContext<SingleCanalBinlog> ctx) {
        while (!canceled.get() && hasNext()) {
            synchronized (ctx.getCheckpointLock()) {
                List<Map<String, Object>> columnMaps = next();
                for (Map<String, Object> columnMap : columnMaps) {
                    ctx.collect(new SingleCanalBinlog(task.getSourceName(), task.getTableName(), -1L, CanalEntry.EventType.INSERT, columnMap, new HashMap<>(), columnMap));
                }
                commit();
                // 连续多次遇到空id区间, 采用jdbc矫正
                if (columnMaps.isEmpty() && currentQueryBatchSize == MAX_QUERY_BATCH_SIZE) {
                    correctByJdbc();
                }
                // 遇到稀疏id区间 / 连续少次遇到空id区间, 适当加大batch
                else if (columnMaps.size() < MIN_QUERY_BATCH_SIZE * 0.5) {
                    currentQueryBatchSize = Math.min(currentQueryBatchSize * 2, MAX_QUERY_BATCH_SIZE);
                }
                // 离开异常id区间, 适当降低batch
                else if (columnMaps.size() > MIN_QUERY_BATCH_SIZE * 1.5) {
                    currentQueryBatchSize = Math.max(currentQueryBatchSize / 2, MIN_QUERY_BATCH_SIZE);
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
                task.getPivot() != DIRECT_SCAN_COMPLETE_FLAG : task.getPivot() <= task.getUpperBound();
    }

    private List<Map<String, Object>> next() {
        StringBuilder sqlBuilder = new StringBuilder(baseSql);
        if (task.getScanMode() == TumblingWindow) {
            sqlBuilder.append(String.format(" AND %s <= id AND id < %s", task.getPivot(), task.getPivot() + currentQueryBatchSize));
        }
        return jdbcTemplate.queryForColumnMaps(sqlBuilder.toString());
    }

    private void commit() {
        long nextPivot = task.getScanMode() == Direct ?
                DIRECT_SCAN_COMPLETE_FLAG : task.getPivot() + currentQueryBatchSize;
        task.setPivot(nextPivot);
    }

    private void correctByJdbc() {
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
