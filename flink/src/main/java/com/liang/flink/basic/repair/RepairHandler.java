package com.liang.flink.basic.repair;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.liang.common.dto.Config;
import com.liang.common.service.DaemonExecutor;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.service.database.template.RedisTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import com.liang.flink.dto.SingleCanalBinlog;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Collector;

import java.sql.ResultSetMetaData;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
@RequiredArgsConstructor
public class RepairHandler extends RichFlatMapFunction<List<RepairSplit>, SingleCanalBinlog> implements CheckpointedFunction {
    private static final ListStateDescriptor<RepairState> LIST_STATE_DESCRIPTOR = new ListStateDescriptor<>(RepairState.class.getSimpleName(), RepairState.class);
    private final Lock lock = new ReentrantLock(true);
    private final Config config;
    private final String repairKey;
    private RedisTemplate redisTemplate;
    private ListState<RepairState> repairStateHolder;
    private RepairState repairState;


    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ConfigUtils.setConfig(config);
        redisTemplate = new RedisTemplate("metadata");
        repairStateHolder = context.getOperatorStateStore().getUnionListState(LIST_STATE_DESCRIPTOR);
    }

    @Override
    public void flatMap(List<RepairSplit> repairSplits, Collector<SingleCanalBinlog> out) throws Exception {
        RepairSplit repairSplit = repairSplits.get(getRuntimeContext().getIndexOfThisSubtask());
        // 初始化state
        repairState = new RepairState(repairSplit, 0L);
        // 更新state
        for (RepairState state : repairStateHolder.get()) {
            if (repairSplit.equals(state.getRepairSplit())) {
                repairState = state;
                break;
            }
        }
        // 初始化sql与jdbcTemplate
        String sql = repairSplit.getSql()
                .WHERE("id >= " + repairState.getPosition())
                .toString();
        JdbcTemplate jdbcTemplate = new JdbcTemplate(repairSplit.getSourceName());
        // 异步执行, 不阻挡barrier
        DaemonExecutor.launch("RepairSender", () ->
                jdbcTemplate.streamQuery(true, sql, rs -> {
                    try {
                        lock.lock();
                        ResultSetMetaData metaData = rs.getMetaData();
                        int columnCount = metaData.getColumnCount();
                        Map<String, Object> columnMap = new HashMap<>(columnCount);
                        for (int i = 1; i <= columnCount; i++)
                            columnMap.put(metaData.getColumnName(i), rs.getString(i));
                        out.collect(new SingleCanalBinlog(metaData.getCatalogName(1), metaData.getTableName(1), 0L, CanalEntry.EventType.INSERT, new HashMap<>(), columnMap));
                        repairState.setPosition(rs.getLong("id"));
                    } finally {
                        lock.unlock();
                    }
                })
        );
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        try {
            lock.lock();
            repairStateHolder.clear();
            repairStateHolder.add(repairState);
            String logs = String.format("repairSplit %s ckp-%d successfully, position: %,d",
                    JsonUtils.toString(repairState.getRepairSplit()),
                    context.getCheckpointId(),
                    repairState.getPosition()
            );
            reportAndLog(logs);
        } finally {
            lock.unlock();
        }
    }

    private void reportAndLog(String logs) {
        logs = String.format("[RepairHandler-%d] %s", getRuntimeContext().getIndexOfThisSubtask(), logs);
        redisTemplate.rPush(repairKey, logs);
        log.info("{}", logs);
    }
}
