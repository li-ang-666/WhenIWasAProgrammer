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
            Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            statement.setFetchSize(Integer.MIN_VALUE);
            statement.setQueryTimeout((int) TimeUnit.HOURS.toSeconds(24));
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
