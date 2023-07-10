package com.liang.flink.basic;

import com.liang.common.dto.Config;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.dto.SubRepairTask;
import com.liang.flink.service.RepairDataHandler;
import com.liang.flink.service.TaskGenerator;
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

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@SuppressWarnings("SynchronizeOnNonFinalField")
public class RepairSource extends RichParallelSourceFunction<SingleCanalBinlog> implements CheckpointedFunction {
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Config config;
    private final String JobClassName;

    private SubRepairTask task;
    private ListState<SubRepairTask> taskState;

    public RepairSource(Config config, String jobClassName) {
        this.config = config;
        this.JobClassName = jobClassName;
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
            log.warn("task-{} restored from taskState, currentId: {}, targetId: {}, queueSize: {}",
                    task.getTaskId(), task.getCurrentId(), task.getTargetId(), task.getPendingQueue().size());
            return;
        }
    }

    @Override
    public void open(Configuration parameters) {
        new Thread(new RepairDataHandler(task, running)).start();
    }

    @Override
    public void run(SourceContext<SingleCanalBinlog> ctx) {
        ConcurrentLinkedQueue<SingleCanalBinlog> queue = task.getPendingQueue();
        while (running.get() || !queue.isEmpty()) {
            int i = queue.size();
            while (i-- > 0) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(queue.poll());
                }
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
        running.set(false);
    }

    @Override
    public void close() {
        cancel();
    }
}
