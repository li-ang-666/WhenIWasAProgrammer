package com.liang.flink.basic.repair;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.liang.common.dto.Config;
import com.liang.common.dto.config.RepairTask;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.dto.SingleCanalBinlog;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Collector;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.sql.ResultSetMetaData;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
public class RepairHandler extends RichFlatMapFunction<RepairSplit, SingleCanalBinlog> implements CheckpointedFunction {
    private final Lock lock = new ReentrantLock(true);
    private final Config config;

    @Override
    public void initializeState(FunctionInitializationContext context) {
        ConfigUtils.setConfig(config);
    }

    @Override
    public void flatMap(RepairSplit repairSplit, Collector<SingleCanalBinlog> out) {
        try {
            lock.lock();
            // 初始化
            RepairTask repairTask = repairSplit.getRepairTask();
            Roaring64Bitmap bitmap = repairSplit.getBitmap();
            JdbcTemplate jdbcTemplate = new JdbcTemplate(repairTask.getSourceName());
            String sql = bitmap.stream().boxed()
                    .map(id -> new SQL().SELECT(repairTask.getColumns())
                            .FROM(repairTask.getTableName())
                            .WHERE(repairTask.getWhere())
                            .WHERE("id = " + id)
                            .toString())
                    .collect(Collectors.joining(") UNION ALL (", "(", ")"));
            // 执行sql
            jdbcTemplate.streamQuery(true, sql, rs -> {
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();
                Map<String, Object> columnMap = new HashMap<>(columnCount);
                for (int i = 1; i <= columnCount; i++) {
                    columnMap.put(metaData.getColumnName(i), rs.getString(i));
                }
                out.collect(new SingleCanalBinlog(metaData.getCatalogName(1), metaData.getTableName(1), 0L, CanalEntry.EventType.INSERT, new HashMap<>(), columnMap));
            });
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) {
        try {
            lock.lock();
        } finally {
            lock.unlock();
        }
    }
}
