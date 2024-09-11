package com.liang.flink.basic.repair;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.liang.common.dto.Config;
import com.liang.common.dto.config.RepairTask;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.service.database.template.RedisTemplate;
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

import java.sql.ResultSetMetaData;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/*
 * https://nightlies.apache.org/flink/flink-docs-release-1.17/zh/docs/dev/datastream/fault-tolerance/checkpointing
 * 部分任务结束后的 Checkpoint
 */
@Slf4j
@RequiredArgsConstructor
public class RepairSource extends RichParallelSourceFunction<SingleCanalBinlog> implements CheckpointedFunction {
    private static final ListStateDescriptor<RepairState> STATE_DESCRIPTOR = new ListStateDescriptor<>(RepairState.class.getSimpleName(), RepairState.class);
    private final RepairState repairState = new RepairState();
    private final Config config;
    private final String repairKey;
    private RedisTemplate redisTemplate;
    private ListState<RepairState> repairStateHolder;

    @Override
    @SneakyThrows
    public void initializeState(FunctionInitializationContext context) {
        redisTemplate = new RedisTemplate("metadata");
        // 根据index分配task
        ConfigUtils.setConfig(config);
        repairState.setRepairTask(config.getRepairTasks().get(getRuntimeContext().getIndexOfThisSubtask()));
        RepairTask repairTask = repairState.getRepairTask();
        // 根据task恢复state
        repairStateHolder = context.getOperatorStateStore().getUnionListState(STATE_DESCRIPTOR);
        for (RepairState repairStateOld : repairStateHolder.get()) {
            RepairTask repairTaskOld = repairStateOld.getRepairTask();
            long maxParsedIdOld = repairStateOld.getMaxParsedId();
            if (repairTask.equals(repairTaskOld)) {
                repairState.setMaxParsedId(maxParsedIdOld);
                String logs = String.format("RepairTask %s restored successfully, last id: %d",
                        JsonUtils.toString(repairTask),
                        repairState.getMaxParsedId()
                );
                reportAndLog(logs);
                break;
            }
        }
    }

    @Override
    public void open(Configuration parameters) {
    }

    @Override
    public void run(SourceContext<SingleCanalBinlog> ctx) {
        RepairTask repairTask;
        JdbcTemplate jdbcTemplate;
        String sql;
        synchronized (ctx.getCheckpointLock()) {
            repairTask = repairState.getRepairTask();
            jdbcTemplate = new JdbcTemplate(repairTask.getSourceName());
            sql = new SQL()
                    .SELECT(repairTask.getColumns())
                    .FROM(repairTask.getTableName())
                    .WHERE(repairTask.getWhere())
                    .WHERE("id >= " + repairState.getMaxParsedId())
                    .ORDER_BY("id ASC")
                    .toString();
            reportAndLog(String.format("repair sql: %s", sql));
        }
        jdbcTemplate.streamQuery(sql, rs -> {
            synchronized (ctx.getCheckpointLock()) {
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();
                Map<String, Object> columnMap = new HashMap<>(columnCount);
                for (int i = 1; i <= columnCount; i++) columnMap.put(metaData.getColumnName(i), rs.getString(i));
                ctx.collect(new SingleCanalBinlog(repairTask.getSourceName(), repairTask.getTableName(), -1L, CanalEntry.EventType.INSERT, new HashMap<>(), columnMap));
                repairState.setMaxParsedId(rs.getLong("id"));
            }
        });
    }

    @Override
    @SneakyThrows
    public void snapshotState(FunctionSnapshotContext context) {
        repairStateHolder.clear();
        repairStateHolder.addAll(Collections.singletonList(repairState));
        String logs = String.format("RepairTask %s ckp-%d successfully, max id: %d",
                JsonUtils.toString(repairState.getRepairTask()),
                context.getCheckpointId(),
                repairState.getMaxParsedId()
        );
        reportAndLog(logs);
    }

    @Override
    public void cancel() {
        System.exit(100);
    }

    private void reportAndLog(String logs) {
        logs = String.format("[RepairSource-%d] %s", getRuntimeContext().getIndexOfThisSubtask(), logs);
        redisTemplate.rPush(repairKey, logs);
        log.info("{}", logs);
    }
}
