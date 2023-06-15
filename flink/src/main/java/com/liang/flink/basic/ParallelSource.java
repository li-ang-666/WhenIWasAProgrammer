package com.liang.flink.basic;

import com.liang.common.dto.Config;
import com.liang.flink.dto.SubRepairTask;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.service.RepairDataHandler;
import com.liang.flink.service.TaskGenerator;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class ParallelSource extends RichParallelSourceFunction<SingleCanalBinlog> implements CheckpointedFunction {
    private final Config config;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private SubRepairTask task;
    private final ConcurrentLinkedQueue<SingleCanalBinlog> queue = new ConcurrentLinkedQueue<>();

    private ListState<SubRepairTask> taskState;
    private ListState<ConcurrentLinkedQueue<SingleCanalBinlog>> queueState;

    public ParallelSource(Config config) {
        this.config = config;
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ConfigUtils.setConfig(config);
        int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
        task = TaskGenerator.generateFrom(config.getRepairTasks().get(indexOfThisSubtask));
        queueState = context.getOperatorStateStore().getUnionListState(new ListStateDescriptor<>("queueState", TypeInformation.of(
                new TypeHint<ConcurrentLinkedQueue<SingleCanalBinlog>>() {
                })));
        taskState = context.getOperatorStateStore().getUnionListState(new ListStateDescriptor<>("taskState", TypeInformation.of(
                new TypeHint<SubRepairTask>() {
                })));
        if (context.isRestored()) {
            queueState.get().
            for (ConcurrentLinkedQueue<SingleCanalBinlog> stateQueue : queueState.get()) {
                queue.addAll(stateQueue);
                log.warn("queue restored from queueState, queue.size: {}", queue.size());
            }
            for (SubRepairTask stateTask : taskState.get()) {
                task.setCurrentId(stateTask.getCurrentId());
                log.warn("task restored from taskState, task.currentId: {}", task.getCurrentId());
            }
        }
        new Thread(new RepairDataHandler(task, queue, running)).start();
    }

    @Override
    public void run(SourceContext<SingleCanalBinlog> ctx) throws Exception {
        while (running.get() || !queue.isEmpty()) {
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
        ConcurrentLinkedQueue<SingleCanalBinlog> copyQueue;
        synchronized (running) {
            copyTask = SerializationUtils.clone(task);
            copyQueue = SerializationUtils.clone(queue);
        }
        taskState.clear();
        queueState.clear();
        taskState.add(copyTask);
        queueState.add(copyQueue);
    }

    @Override
    public void cancel() {
        running.set(false);
    }
}
