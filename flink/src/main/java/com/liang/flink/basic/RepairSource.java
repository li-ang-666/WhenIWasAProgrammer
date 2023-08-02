package com.liang.flink.basic;

import com.liang.common.dto.Config;
import com.liang.common.service.DaemonExecutor;
import com.liang.common.service.database.template.RedisTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.dto.SubRepairTask;
import com.liang.flink.service.RepairDataHandler;
import com.liang.flink.service.TaskGenerator;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@SuppressWarnings("SynchronizeOnNonFinalField")
/*
 * https://nightlies.apache.org/flink/flink-docs-release-1.17/zh/docs/dev/datastream/fault-tolerance/checkpointing
 * 部分任务结束后的 Checkpoint
 */
public class RepairSource extends RichParallelSourceFunction<SingleCanalBinlog> implements CheckpointedFunction {
    private final static int CHECK_INTERVAL = 1000 * 30;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicBoolean canceled = new AtomicBoolean(false);
    private final Config config;
    private final String repairKey;

    private SubRepairTask task;
    private ListState<SubRepairTask> taskState;

    private RedisTemplate redisTemplate;

    public RepairSource(Config config, String repairKey) {
        this.config = config;
        this.repairKey = repairKey;
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ConfigUtils.setConfig(config);
        int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
        SubRepairTask initTask = TaskGenerator.generateFrom(config.getRepairTasks().get(indexOfThisSubtask));
        task = initTask;
        taskState = context.getOperatorStateStore().getUnionListState(new ListStateDescriptor<>("taskState", TypeInformation.of(
                new TypeHint<SubRepairTask>() {
                })));
        if (!context.isRestored()) {
            return;
        }
        for (SubRepairTask stateTask : taskState.get()) {
            if (!stateTask.getTaskId().equals(task.getTaskId())) {
                continue;
            }
            task = stateTask;
            // 程序重启, targetId用最新的, select、where条件用最新的
            task.setTargetId(initTask.getTargetId());
            task.setColumns(initTask.getColumns());
            task.setWhere(initTask.getWhere());
            log.warn("task-{} restored from taskState, sql: select {} from {} where {}, currentId: {}, targetId: {}, queueSize: {}",
                    task.getTaskId(),
                    task.getColumns(), task.getTableName(), task.getWhere(),
                    task.getCurrentId(), task.getTargetId(), task.getPendingQueue().size()
            );
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
        keepSend(ctx);
        registerComplete();
        waitingAllComplete();
    }

    private void keepSend(SourceContext<SingleCanalBinlog> ctx) {
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

    private void registerComplete() {
        redisTemplate.hSet(repairKey, task.getTaskId(),
                String.format("[completed] currentId: %s", task.getCurrentId())
        );
    }

    @SneakyThrows(InterruptedException.class)
    private void waitingAllComplete() {
        while (!canceled.get()) {
            TimeUnit.MILLISECONDS.sleep(CHECK_INTERVAL);
            Map<String, String> reportMap = redisTemplate.hScan(repairKey);
            long completedNum = reportMap.values().stream().filter(e -> e.startsWith("[completed]")).count();
            long totalNum = config.getRepairTasks().size();
            if (completedNum == totalNum) {
                log.info("Detected all repair task has been completed, RepairTask-{} will be cancel after the next checkpoint", task.getTaskId());
                cancel();
            }
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        SubRepairTask copyTask;
        synchronized (task) {
            copyTask = SerializationUtils.clone(task);
        }
        taskState.clear();
        taskState.add(copyTask);
    }

    @Override
    public void cancel() {
        redisTemplate.del(repairKey);
        running.set(false);
        canceled.set(true);
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
