package com.liang.flink.basic.repair;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.liang.common.dto.Config;
import com.liang.common.dto.config.RepairTask;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.dto.SingleCanalBinlog;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Collector;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.sql.ResultSetMetaData;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

@Slf4j
@RequiredArgsConstructor
public class RepairHandler extends RichFlatMapFunction<RepairSplit, SingleCanalBinlog> implements CheckpointedFunction {
    private static final ListStateDescriptor<Roaring64Bitmap> LIST_STATE_DESCRIPTOR = new ListStateDescriptor<>("RepairHandlerBitmap", Roaring64Bitmap.class);

    private final int subtaskId = getRuntimeContext().getIndexOfThisSubtask();
    private final Roaring64Bitmap bitmap = new Roaring64Bitmap();
    private final Config config;
    private JdbcTemplate jdbcTemplate;


    @Override
    @SneakyThrows
    public void initializeState(FunctionInitializationContext context) {
        ListState<Roaring64Bitmap> unionListState = context.getOperatorStateStore().getUnionListState(LIST_STATE_DESCRIPTOR);
        Iterator<Roaring64Bitmap> iterator = unionListState.get().iterator();
        while (iterator.hasNext()) {
            Roaring64Bitmap bitmap = iterator.next();
        }

    }

    @Override
    public void open(Configuration parameters) {
        ConfigUtils.setConfig(config);
        for (RepairTask repairTask : config.getRepairTasks()) {
            if (repairTask.getChannels().contains(subtaskId)) {
                jdbcTemplate = new JdbcTemplate(repairTask.getSourceName());
                return;
            }
        }
    }

    @Override
    public void flatMap(RepairSplit repairSplit, Collector<SingleCanalBinlog> out) {
        doQuery(repairSplit, out);
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
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

}
