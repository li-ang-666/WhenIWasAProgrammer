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

    private SubRepairTask task;
    private ListState<SubRepairTask> taskState;

    public RepairSource(Config config) {
        this.config = config;
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ConfigUtils.setConfig(config);
        int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
        task = TaskGenerator.generateFrom(config.getRepairTasks().get(indexOfThisSubtask));
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
            log.warn("task-{} restored from taskState, currentId: {}, targetId: {}, queueSize: {}",
                    task.getTaskId(), task.getCurrentId(), task.getTargetId(), task.getPendingQueue().size());
            return;
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        new Thread(new RepairDataHandler(task)).start();
    }

    @Override
    public void run(SourceContext<SingleCanalBinlog> ctx) throws Exception {
        ConcurrentLinkedQueue<SingleCanalBinlog> queue = task.getPendingQueue();
        while (running.get()) {
            int i = Math.min(1024, queue.size());
            synchronized (ctx.getCheckpointLock()) {
                while (i-- > 0) {
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
    public void close() throws Exception {
        cancel();
        ConfigUtils.closeAll();
    }
}
