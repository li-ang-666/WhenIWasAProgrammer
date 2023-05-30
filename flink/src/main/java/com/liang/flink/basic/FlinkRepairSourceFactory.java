package com.liang.flink.basic;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.liang.common.dto.Config;
import com.liang.common.dto.SubRepairTask;
import com.liang.common.dto.config.RepairTask;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.service.RepairTaskDataHandler;
import com.liang.flink.service.TaskGenerator;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FlinkRepairSourceFactory {
    private FlinkRepairSourceFactory() {
    }

    public static List<FlinkRepairSource> create() {
        List<FlinkRepairSource> result = new ArrayList<>();
        List<RepairTask> repairTasks = ConfigUtils.getConfig().getRepairTasks();
        for (RepairTask repairTask : repairTasks) {
            SubRepairTask subRepairTask = TaskGenerator.generateFrom(repairTask);
            if (subRepairTask != null)
                result.add(new FlinkRepairSource(ConfigUtils.getConfig(), subRepairTask));
        }
        return result;
    }

    @Slf4j
    public static class FlinkRepairSource extends RichSourceFunction<SingleCanalBinlog> implements CheckpointedFunction {
        private final Config config;
        private final @Getter SubRepairTask task;
        private RepairTaskDataHandler dataHandler;
        private ListState<Long> state;

        public FlinkRepairSource(Config config, SubRepairTask task) {
            this.config = config;
            this.task = task;
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            //非parallelSource接口, 不需要unionList
            state = context.getOperatorStateStore().getListState(new ListStateDescriptor<>("state", Long.class));
            if (context.isRestored()) {
                for (long l : state.get()) {
                    log.warn("restored from checkpoint, 调整 currentId: {} -> {}", task.getCurrentId(), l);
                    task.setCurrentId(l);
                }
            }
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            ConfigUtils.setConfig(config);
            dataHandler = new RepairTaskDataHandler(task);
        }

        @Override
        public void run(SourceContext<SingleCanalBinlog> ctx) throws Exception {
            do {
                for (Map<String, Object> columnMap : dataHandler.nextBatch())
                    ctx.collect(new SingleCanalBinlog(task.getSourceName(), task.getTableName(), -1L, CanalEntry.EventType.INSERT, columnMap));
                dataHandler.commit();
            } while (dataHandler.hasMore());
//            log.info("{} 执行完毕, 当前 source 线程将被挂起", task);
//            while (true) ;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            state.clear();
            state.add(task.getCurrentId());
        }

        @Override
        public void cancel() {
        }
    }
}
