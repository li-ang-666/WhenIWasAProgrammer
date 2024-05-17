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
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
@RequiredArgsConstructor
public class RepairHandler extends RichFlatMapFunction<RepairSplit, SingleCanalBinlog> {
    private final Config config;
    private RepairTask repairTaskOfThisSubtask;
    private JdbcTemplate jdbcTemplate;

    @Override
    public void open(Configuration parameters) {
        ConfigUtils.setConfig(config);
        int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
        repairTaskOfThisSubtask = config.getRepairTasks().parallelStream()
                .filter(repairTask -> repairTask.getChannels().contains(indexOfThisSubtask))
                .findFirst()
                .orElseThrow(RuntimeException::new);
        jdbcTemplate = new JdbcTemplate(repairTaskOfThisSubtask.getSourceName());
    }

    @Override
    public void flatMap(RepairSplit repairSplit, Collector<SingleCanalBinlog> out) {
        try {
            log.info("get repairSplit: {}", repairSplit);
            TimeUnit.HOURS.sleep(1);
        } catch (Exception ignore) {
        }
        List<Map<String, Object>> columnMaps = jdbcTemplate.queryForColumnMaps(repairSplit.getSql());
        for (Map<String, Object> columnMap : columnMaps) {
            out.collect(new SingleCanalBinlog(repairTaskOfThisSubtask.getSourceName(), repairTaskOfThisSubtask.getTableName(), -1L, CanalEntry.EventType.INSERT, columnMap, new HashMap<>(), columnMap));
        }
    }
}
