package com.liang.flink.basic.repair;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.liang.common.dto.Config;
import com.liang.common.dto.config.RepairTask;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.dto.SingleCanalBinlog;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Collector;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
@RequiredArgsConstructor
public class RepairHandler extends RichFlatMapFunction<RepairSplit, SingleCanalBinlog> implements CheckpointedFunction {
    private static final boolean AUTO_COMMIT = false;
    private static final int RESULT_SET_TYPE = ResultSet.TYPE_FORWARD_ONLY;
    private static final int RESULT_SET_CONCURRENCY = ResultSet.CONCUR_READ_ONLY;
    private static final int FETCH_SIZE = Integer.MIN_VALUE;
    private static final int TIME_OUT = (int) TimeUnit.HOURS.toSeconds(24);
    private final ReentrantLock lock = new ReentrantLock(true);
    private final Config config;
    private JdbcTemplate jdbcTemplate;

    @Override
    public void initializeState(FunctionInitializationContext context) {
    }

    @Override
    public void open(Configuration parameters) {
        ConfigUtils.setConfig(config);
        int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
        for (RepairTask repairTask : config.getRepairTasks()) {
            if (repairTask.getChannels().contains(indexOfThisSubtask)) {
                jdbcTemplate = new JdbcTemplate(repairTask.getSourceName());
                return;
            }
        }
    }

    @Override
    public void flatMap(RepairSplit repairSplit, Collector<SingleCanalBinlog> out) {
        lock.lock();
        doQuery(repairSplit, out);
        lock.unlock();
    }

    private void doQuery(RepairSplit repairSplit, Collector<SingleCanalBinlog> out) {
        jdbcTemplate.streamQuery(repairSplit.getSql(), rs -> {
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            Map<String, Object> columnMap = new HashMap<>(columnCount);
            for (int i = 1; i <= columnCount; i++) {
                columnMap.put(metaData.getColumnName(i), rs.getString(i));
            }
            out.collect(new SingleCanalBinlog(repairSplit.getSourceName(), repairSplit.getTableName(), -1L, CanalEntry.EventType.INSERT, new HashMap<>(), columnMap));
        });
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) {
        lock.lock();
        lock.unlock();
    }
}
