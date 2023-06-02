package com.liang.flink.service;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.liang.common.dto.SubRepairTask;
import com.liang.common.dto.config.RepairTask;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.flink.dto.SingleCanalBinlog;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.liang.common.dto.config.RepairTask.ScanMode.Direct;
import static com.liang.common.dto.config.RepairTask.ScanMode.TumblingWindow;


@Slf4j
public class RepairDataHandler implements Runnable {
    private final static int BATCH_SIZE = 200;
    private final static int MAX_QUEUE_SIZE = 1000000;

    private final ConcurrentLinkedQueue<SingleCanalBinlog> queue;
    private final JdbcTemplate jdbcTemplate;
    private final SubRepairTask task;
    private final String basicSql;

    private volatile long highWatermark;

    public RepairDataHandler(SubRepairTask task, ConcurrentLinkedQueue<SingleCanalBinlog> queue) {
        this.queue = queue;
        this.jdbcTemplate = new JdbcTemplate(task.getSourceName());
        this.task = task;
        basicSql = String.format("select %s from %s where %s ",
                task.getColumns(), task.getTableName(), task.getWhere());
    }

    @Override
    public void run() {
        while (hasNextBatch()) {
            if (task.getScanMode() == Direct || queue.size() < MAX_QUEUE_SIZE) {
                List<Map<String, Object>> columnMaps = nextBatch();
                for (Map<String, Object> columnMap : columnMaps) {
                    queue.add(new SingleCanalBinlog(task.getSourceName(), task.getTableName(), -1L, CanalEntry.EventType.INSERT, columnMap));
                }
                commit();
            }
        }
    }

    private boolean hasNextBatch() {
        RepairTask.ScanMode scanMode = task.getScanMode();
        if (scanMode == Direct) {
            return task.getCurrentId() != -1;
        } else {
            return task.getCurrentId() < task.getTargetId();
        }
    }

    private List<Map<String, Object>> nextBatch() {
        String sql = basicSql;
        if (task.getScanMode() == TumblingWindow) {
            highWatermark = Math.min(task.getCurrentId() + BATCH_SIZE, task.getTargetId());
            sql += String.format(" and %s <= id and id < %s", task.getCurrentId(), highWatermark);
        }
        return jdbcTemplate.queryForColumnMapList(sql);
    }

    private void commit() {
        if (task.getScanMode() == Direct) {
            task.setCurrentId(-1);
        } else {
            task.setCurrentId(highWatermark);
        }
    }
}

