package com.liang.flink.basic.repair;

import com.liang.common.dto.Config;
import com.liang.common.dto.config.RepairTask;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import com.liang.flink.dto.SingleCanalBinlog;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/*
 * https://nightlies.apache.org/flink/flink-docs-release-1.17/zh/docs/dev/datastream/fault-tolerance/checkpointing
 * 部分任务结束后的 Checkpoint
 */
@Slf4j
@RequiredArgsConstructor
public class RepairSource extends RichParallelSourceFunction<SingleCanalBinlog> implements CheckpointedFunction {
    private static final ListStateDescriptor<RepairState> STATE_DESCRIPTOR = new ListStateDescriptor<>(RepairState.class.getSimpleName(), RepairState.class);
    private final Lock lock = new ReentrantLock(true);
    private final AtomicBoolean canceled = new AtomicBoolean(false);
    private final RepairState repairState = new RepairState();
    private final Config config;
    private ListState<RepairState> listState;

    @Override
    @SneakyThrows
    public void initializeState(FunctionInitializationContext context) {
        // 根据index分配task
        ConfigUtils.setConfig(config);
        repairState.setRepairTask(config.getRepairTasks().get(getRuntimeContext().getIndexOfThisSubtask()));
        // 根据task恢复bitmap
        listState = context.getOperatorStateStore().getUnionListState(STATE_DESCRIPTOR);
        Iterable<RepairState> iterable = listState.get();
        while (iterable.iterator().hasNext()) {
            RepairState repairStateOld = iterable.iterator().next();
            if (repairState.getRepairTask().equals(repairStateOld.getRepairTask())) {
                repairState.setBitmap(repairStateOld.getBitmap());
                log.info("RepairTask {} restored successfully, bitmap size: {}",
                        JsonUtils.toString(repairState.getRepairTask()), repairState.getBitmap().getLongCardinality());
                return;
            }
        }
    }

    @Override
    public void open(Configuration parameters) {
    }

    @Override
    public void run(SourceContext<SingleCanalBinlog> ctx) {
        RepairTask repairTask = repairState.getRepairTask();
        JdbcTemplate jdbcTemplate = new JdbcTemplate(repairTask.getSourceName());
        String sql = new SQL()
                .SELECT(repairTask.getColumns())
                .FROM(repairTask.getTableName())
                .WHERE(repairTask.getWhere())
                .toString();
        jdbcTemplate.streamQuery(sql, rs -> {
            if (canceled.get()) {
                return;
            }
            long id = Long.parseLong(rs.getString("id"));
            synchronized (ctx.getCheckpointLock()) {
                lock.lock();
                Roaring64Bitmap bitmap = repairState.getBitmap();
                if (!bitmap.contains(id)) {
                    ctx.collect(null);
                    bitmap.add(id);
                }
                lock.unlock();
            }
        });
        cancel();
    }

    @Override
    @SneakyThrows
    public void snapshotState(FunctionSnapshotContext context) {
        lock.lock();
        listState.clear();
        listState.addAll(Collections.singletonList(repairState));
        lock.unlock();
    }

    @Override
    public void cancel() {
        canceled.set(true);
    }
}
