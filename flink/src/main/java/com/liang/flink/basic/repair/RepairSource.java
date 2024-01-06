package com.liang.flink.basic.repair;

import com.liang.common.dto.Config;
import com.liang.common.service.DaemonExecutor;
import com.liang.common.service.database.template.RedisTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.dto.SubRepairTask;
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

import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

/*
 * https://nightlies.apache.org/flink/flink-docs-release-1.17/zh/docs/dev/datastream/fault-tolerance/checkpointing
 * 部分任务结束后的 Checkpoint
 */
@Slf4j
@RequiredArgsConstructor
public class RepairSource extends RichParallelSourceFunction<SingleCanalBinlog> implements CheckpointedFunction {
    // bigger than report interval
    private static final int CHECK_COMPLETE_INTERVAL_MILLISECONDS = 1000 * 60;
    private static final String TASK_STATE_NAME = "TASK_STATE";
    private static final ListStateDescriptor<SubRepairTask> TASK_STATE_DESCRIPTOR = new ListStateDescriptor<>(TASK_STATE_NAME, SubRepairTask.class);
    private static final String RUNNING_REPORT_PREFIX = "[checkpoint]";
    private static final String COMPLETE_REPORT_PREFIX = "[completed]";
    // data handler thread
    private final AtomicBoolean running = new AtomicBoolean(true);
    // flink web ui cancel
    private final AtomicBoolean canceled = new AtomicBoolean(false);
    // self completed
    private final AtomicBoolean selfCompleted = new AtomicBoolean(false);
    private final Config config;
    private final String repairKey;
    private volatile SubRepairTask task;
    private ListState<SubRepairTask> taskState;
    private RedisTemplate redisTemplate;

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // 初始化task与state
        ConfigUtils.setConfig(config);
        task = TaskGenerator.generateFrom(config.getRepairTasks().get(getRuntimeContext().getIndexOfThisSubtask()));
        taskState = context.getOperatorStateStore().getUnionListState(TASK_STATE_DESCRIPTOR);
        // 从ckp恢复task
        for (SubRepairTask stateTask : taskState.get()) {
            if (!stateTask.getTaskId().equals(task.getTaskId())) continue;
            // 恢复queue
            task.getPendingQueue().addAll(stateTask.getPendingQueue());
            // 跳过id
            task.setCurrentId(stateTask.getCurrentId());
            log.warn("task-{} restored from taskState, sql: select {} from {} where {}, currentId: {}, targetId: {}, queueSize: {}",
                    task.getTaskId(),
                    task.getColumns(), task.getTableName(), task.getWhere(),
                    task.getCurrentId(), task.getTargetId(), task.getPendingQueue().size());
            return;
        }
    }

    @Override
    public void open(Configuration parameters) {
        redisTemplate = new RedisTemplate("metadata");
        DaemonExecutor.launch("RepairDataHandler", new RepairDataHandler(task, running, repairKey));
    }

    @Override
    public void run(SourceContext<SingleCanalBinlog> ctx) {
        keepSending(ctx);
        registerSelfComplete();
        waitingAllComplete();
    }

    // step 1
    private void keepSending(SourceContext<SingleCanalBinlog> ctx) {
        ConcurrentLinkedQueue<SingleCanalBinlog> queue = task.getPendingQueue();
        while (!canceled.get() && (running.get() || !queue.isEmpty())) {
            int i = queue.size();
            while (i-- > 0) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(queue.poll());
                }
            }
        }
    }

    // step 2
    private void registerSelfComplete() {
        String info = String.format("%s currentId: %s", COMPLETE_REPORT_PREFIX, task.getCurrentId());
        // the last time to write redis
        redisTemplate.hSet(repairKey, task.getTaskId(), info);
        selfCompleted.set(true);
    }

    // step 3
    private void waitingAllComplete() {
        while (!canceled.get()) {
            LockSupport.parkUntil(System.currentTimeMillis() + CHECK_COMPLETE_INTERVAL_MILLISECONDS);
            Map<String, String> repairMap = redisTemplate.hScan(repairKey);
            long numCompleted = repairMap.values().stream().filter(e -> e.startsWith(COMPLETE_REPORT_PREFIX)).count();
            // the first one detected will delete the map, so map maybe empty
            // otherwise, the repair map will always be nonempty
            if (repairMap.isEmpty() || numCompleted == config.getRepairTasks().size()) {
                log.info("detected all repair task has been completed, RepairTask-{} will be cancel after the next checkpoint", task.getTaskId());
                cancel();
            }
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        SubRepairTask copyTask;
        synchronized (repairKey) {
            copyTask = SerializationUtils.clone(task);
        }
        taskState.clear();
        taskState.add(copyTask);
        // registerSelfComplete() is the last time to write redis
        if (selfCompleted.get()) return;
        String info = String.format("%s currentId: %s, targetId: %s, lag: %s, queueSize: %s", RUNNING_REPORT_PREFIX,
                copyTask.getCurrentId(), copyTask.getTargetId(), copyTask.getTargetId() - copyTask.getCurrentId(), copyTask.getPendingQueue().size());
        redisTemplate.hSet(repairKey, copyTask.getTaskId(), info);
    }

    @Override
    public void cancel() {
        running.set(false);
        canceled.set(true);
        redisTemplate.del(repairKey);
    }

    /**
     * 任务结束的时候, 是从Source开始执行close()
     * 为了不影响下游算子, 在Source端不释放连接池资源
     */
    @Override
    public void close() {
        cancel();
    }
}
