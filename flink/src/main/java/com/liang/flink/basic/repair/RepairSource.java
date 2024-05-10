package com.liang.flink.basic.repair;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.liang.common.dto.Config;
import com.liang.common.dto.config.RepairTask;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.service.database.template.RedisTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import com.liang.flink.dto.SingleCanalBinlog;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.SerializationUtils;
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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

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
    private static final int QUERY_BATCH_SIZE = 1024;
    private static final int DIRECT_SCAN_COMPLETE_FLAG = -1;
    // lock
    private final Lock lock = new ReentrantLock(true);
    // flink web ui cancel
    private final AtomicBoolean canceled = new AtomicBoolean(false);
    private final Config config;
    private final String repairKey;
    private volatile RepairTask task;
    private ListState<RepairTask> taskState;
    private RedisTemplate redisTemplate;
    private String baseSql;
    private JdbcTemplate jdbcTemplate;

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // 初始化task与state
        ConfigUtils.setConfig(config);
        task = config.getRepairTasks().get(getRuntimeContext().getIndexOfThisSubtask());
        taskState = context.getOperatorStateStore().getUnionListState(TASK_STATE_DESCRIPTOR);
        // 从ckp恢复task
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
        baseSql = String.format("select %s from %s where %s", task.getColumns(), task.getTableName(), task.getWhere());
        redisTemplate = new RedisTemplate("metadata");
        jdbcTemplate = new JdbcTemplate(task.getSourceName());
    }

    @Override
    public void run(SourceContext<SingleCanalBinlog> ctx) {
        while (!canceled.get() && hasNext()) {
            lock.lock();
            for (Map<String, Object> columnMap : next()) {
                ctx.collect(new SingleCanalBinlog(task.getSourceName(), task.getTableName(), -1L, CanalEntry.EventType.INSERT, columnMap, new HashMap<>(), columnMap));
            }
            commit();
            lock.unlock();
        }
        registerSelfComplete();
        waitingAllComplete();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        lock.lock();
        RepairTask copyTask = SerializationUtils.clone(task);
        taskState.clear();
        taskState.add(copyTask);
        if (hasNext()) {
            Long pivot = copyTask.getPivot();
            Long upperBound = copyTask.getUpperBound();
            String info = String.format("%s pivot: %s, upperBound: %s, lag: %s",
                    RUNNING_REPORT_PREFIX, pivot, upperBound, upperBound - pivot);
            redisTemplate.hSet(repairKey, String.valueOf(copyTask.getTaskId()), info);
        }
        lock.unlock();
    }

    @Override
    public void cancel() {
        canceled.set(true);
    }

    @Override
    public void close() {
        cancel();
    }

    private boolean hasNext() {
        return task.getScanMode() == Direct ?
                task.getPivot() != DIRECT_SCAN_COMPLETE_FLAG : task.getPivot() <= task.getUpperBound();
    }

    private List<Map<String, Object>> next() {
        StringBuilder sqlBuilder = new StringBuilder(baseSql);
        if (task.getScanMode() == TumblingWindow) {
            sqlBuilder.append(String.format(" and %s <= id and id < %s", task.getPivot(), task.getPivot() + QUERY_BATCH_SIZE));
        }
        return jdbcTemplate.queryForColumnMaps(sqlBuilder.toString());
    }

    private void commit() {
        task.setPivot(task.getScanMode() == Direct ? DIRECT_SCAN_COMPLETE_FLAG : task.getPivot() + QUERY_BATCH_SIZE);
    }

    private void registerSelfComplete() {
        String info = String.format("%s final pivot: %s", COMPLETE_REPORT_PREFIX, task.getUpperBound());
        redisTemplate.hSet(repairKey, String.valueOf(task.getTaskId()), info);
    }

    private void waitingAllComplete() {
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
}
