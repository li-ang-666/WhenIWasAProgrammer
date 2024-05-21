package com.liang.flink.basic.repair;

import com.liang.common.dto.Config;
import com.liang.common.dto.config.RepairTask;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.dto.SingleCanalBinlog;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

@Slf4j
@RequiredArgsConstructor
public class RepairHandler extends RichFlatMapFunction<RepairSplit, SingleCanalBinlog> {
    private final Config config;
    private int indexOfThisSubtask;
    private JdbcTemplate jdbcTemplate;

    @Override
    public void open(Configuration parameters) {
        ConfigUtils.setConfig(config);
        indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
        for (RepairTask repairTask : config.getRepairTasks()) {
            if (repairTask.getChannels().contains(indexOfThisSubtask)) {
                jdbcTemplate = new JdbcTemplate(repairTask.getSourceName());
                return;
            }
        }
    }

    @Override
    public void flatMap(RepairSplit repairSplit, Collector<SingleCanalBinlog> out) {
        //if (repairSplit.getChannel() != indexOfThisSubtask) {
        //    return;
        //}
        //List<Map<String, Object>> columnMaps = jdbcTemplate.queryForColumnMaps(repairSplit.getSql());
        //for (Map<String, Object> columnMap : columnMaps) {
        //    out.collect(new SingleCanalBinlog(repairSplit.getSourceName(), repairSplit.getTableName(), -1L, CanalEntry.EventType.INSERT, columnMap, new HashMap<>(), columnMap));
        //}
    }
}
