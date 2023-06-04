package com.liang.flink.basic;

import com.liang.common.dto.Config;
import com.liang.common.dto.SubRepairTask;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.service.RepairDataHandler;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

@Slf4j
public class RepairSource extends RichSourceFunction<SingleCanalBinlog> implements CheckpointedFunction {
    private final Config config;

    private final @Getter SubRepairTask task;
    private final ConcurrentLinkedQueue<SingleCanalBinlog> queue = new ConcurrentLinkedQueue<>();
    private ListState<SubRepairTask> taskState;
    private ListState<ConcurrentLinkedQueue<SingleCanalBinlog>> queueState;

    private volatile boolean canceled = false;

    public RepairSource(Config config, SubRepairTask task) {
        this.config = config;
        this.task = task;
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        queueState = context.getOperatorStateStore().getListState(new ListStateDescriptor<>("queueState", TypeInformation.of(
                new TypeHint<ConcurrentLinkedQueue<SingleCanalBinlog>>() {
                })));
        taskState = context.getOperatorStateStore().getListState(new ListStateDescriptor<>("taskState", TypeInformation.of(
                new TypeHint<SubRepairTask>() {
                })));
        if (!context.isRestored())
            return;
        for (ConcurrentLinkedQueue<SingleCanalBinlog> stateQueue : queueState.get()) {
            queue.addAll(stateQueue);
            log.warn("queue restored from queueState, queue.size: {}", queue.size());
        }
        for (SubRepairTask stateTask : taskState.get()) {
            task.setCurrentId(stateTask.getCurrentId());
            log.warn("task restored from taskState, task.currentId: {}", task.getCurrentId());
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ConfigUtils.setConfig(config);
    }

    @Override
    public void run(SourceContext<SingleCanalBinlog> ctx) throws Exception {
        new Thread(new RepairDataHandler(task, queue)).start();
        while (!canceled) {
            if (queue.peek() != null) {
                ctx.collect(queue.poll());
                TimeUnit.MILLISECONDS.sleep(1);
            }
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        taskState.clear();
        queueState.clear();
        //先快照task,再queue
        taskState.add(task);
        queueState.add(queue);
    }

    @Override
    public void cancel() {
        canceled = true;
    }
}
