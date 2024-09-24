package com.liang.flink.basic.repair;

import cn.hutool.core.util.ObjUtil;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.liang.common.dto.Config;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.service.database.template.RedisTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import com.liang.flink.dto.SingleCanalBinlog;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.sql.ResultSetMetaData;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
 * https://nightlies.apache.org/flink/flink-docs-release-1.17/zh/docs/dev/datastream/fault-tolerance/checkpointing
 * 部分任务结束后的 Checkpoint
 */
@Slf4j
@RequiredArgsConstructor
public class RepairSource extends RichParallelSourceFunction<SingleCanalBinlog> implements CheckpointedFunction {
    private static final ListStateDescriptor<RepairState> LIST_STATE_DESCRIPTOR = new ListStateDescriptor<>(RepairState.class.getSimpleName(), RepairState.class);
    private final Config config;
    private final String repairReportKey;
    private final String repairFinishKey;
    private final List<RepairSplit> repairSplits;
    private RedisTemplate redisTemplate;
    private ListState<RepairState> repairStateHolder;
    private RepairState repairState;

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ConfigUtils.setConfig(config);
        redisTemplate = new RedisTemplate("metadata");
        RepairSplit repairSplit = repairSplits.get(getRuntimeContext().getIndexOfThisSubtask());
        // 初始化state
        repairState = new RepairState(repairSplit, 0L);
        // 更新state
        repairStateHolder = context.getOperatorStateStore().getUnionListState(LIST_STATE_DESCRIPTOR);
        for (RepairState state : repairStateHolder.get()) {
            RepairSplit stateSplit = state.getRepairSplit();
            if (repairSplit.getSql().toString().equals(stateSplit.getSql().toString()) && repairSplit.getSourceName().equals(stateSplit.getSourceName())) {
                repairState = state;
                String logs = String.format("RepairSplit %s restored successfully, position: %,d",
                        JsonUtils.toString(repairState.getRepairSplit()),
                        repairState.getPosition()
                );
                reportAndLog(logs);
                break;
            }
        }
    }

    @Override
    public void run(SourceContext<SingleCanalBinlog> ctx) {
        RepairSplit repairSplit = repairState.getRepairSplit();
        long position = repairState.getPosition();
        // 初始化sql与jdbcTemplate
        String sql = ObjUtil.cloneByStream(repairSplit.getSql())
                .WHERE("id >= " + position)
                .toString();
        JdbcTemplate jdbcTemplate = new JdbcTemplate(repairSplit.getSourceName());
        // 执行
        jdbcTemplate.streamQuery(true, sql, rs -> {
            synchronized (ctx.getCheckpointLock()) {
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();
                Map<String, Object> columnMap = new HashMap<>(columnCount);
                for (int i = 1; i <= columnCount; i++)
                    columnMap.put(metaData.getColumnName(i), rs.getString(i));
                ctx.collect(new SingleCanalBinlog(metaData.getCatalogName(1), metaData.getTableName(1), 0L, CanalEntry.EventType.INSERT, new HashMap<>(), columnMap));
                repairState.setPosition(rs.getLong("id"));
            }
        });
        if (repairSplits.size() > 1) {

        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        repairStateHolder.clear();
        repairStateHolder.add(repairState);
        String logs = String.format("RepairSplit %s ckp-%04d successfully, position: %,d",
                JsonUtils.toString(repairState.getRepairSplit()),
                context.getCheckpointId(),
                repairState.getPosition()
        );
        reportAndLog(logs);
    }

    @Override
    public void cancel() {
        System.exit(1);
    }

    private void reportAndLog(String logs) {
        logs = String.format("[RepairSource-%03d] %s", getRuntimeContext().getIndexOfThisSubtask(), logs);
        redisTemplate.rPush(repairReportKey, logs);
        log.info("{}", logs);
    }
}
