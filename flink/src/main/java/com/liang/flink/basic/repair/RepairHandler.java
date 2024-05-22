package com.liang.flink.basic.repair;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.liang.common.dto.Config;
import com.liang.common.dto.config.RepairTask;
import com.liang.common.service.database.holder.DruidHolder;
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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
@RequiredArgsConstructor
public class RepairHandler extends RichFlatMapFunction<RepairSplit, SingleCanalBinlog> implements CheckpointedFunction {
    private static final int FETCH_SIZE = 1000;
    private static final int TIME_OUT = (int) TimeUnit.HOURS.toSeconds(24);
    private static final int RESULT_SET_TYPE = ResultSet.TYPE_FORWARD_ONLY;
    private static final int RESULT_SET_CONCURRENCY = ResultSet.CONCUR_READ_ONLY;
    private final ReentrantLock lock = new ReentrantLock(true);
    private final Config config;
    private DruidDataSource druidDataSource;

    @Override
    public void initializeState(FunctionInitializationContext context) {
    }

    @Override
    public void open(Configuration parameters) {
        ConfigUtils.setConfig(config);
        int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
        for (RepairTask repairTask : config.getRepairTasks()) {
            if (repairTask.getChannels().contains(indexOfThisSubtask)) {
                druidDataSource = new DruidHolder().getPool(repairTask.getSourceName());
                return;
            }
        }
    }

    @Override
    public void flatMap(RepairSplit repairSplit, Collector<SingleCanalBinlog> out) {
        lock.lock();
        try (Connection connection = druidDataSource.getConnection()) {
            connection.setAutoCommit(false);
            Statement statement = connection.createStatement(RESULT_SET_TYPE, RESULT_SET_CONCURRENCY);
            statement.setFetchSize(FETCH_SIZE);
            statement.setQueryTimeout(TIME_OUT);
            ResultSet resultSet = statement.executeQuery(repairSplit.getSql());
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            while (resultSet.next()) {
                Map<String, Object> columnMap = new HashMap<>(columnCount);
                for (int i = 1; i <= columnCount; i++) {
                    columnMap.put(metaData.getColumnName(i), resultSet.getString(i));
                }
                out.collect(new SingleCanalBinlog(repairSplit.getSourceName(), repairSplit.getTableName(), -1L, CanalEntry.EventType.INSERT, columnMap, new HashMap<>(), columnMap));
            }
        } catch (Exception e) {
            log.error("repair split execute error: {}", repairSplit, e);
        }
        lock.unlock();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) {
        lock.lock();
        lock.unlock();
    }
}
