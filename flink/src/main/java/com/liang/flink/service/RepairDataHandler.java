package com.liang.flink.service;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.liang.common.dto.SubRepairTask;
import com.liang.common.dto.config.RepairTask;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.flink.dto.SingleCanalBinlog;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.liang.common.dto.config.RepairTask.ScanMode.Direct;
import static com.liang.common.dto.config.RepairTask.ScanMode.TumblingWindow;


@Slf4j
public class RepairDataHandler implements Runnable {
    private final static int BATCH_SIZE = 1000;
    private final static int MAX_QUEUE_SIZE = 100000;
    private final static int DIRECT_TASK_FINISH_ID = 404;

    private final ConcurrentLinkedQueue<SingleCanalBinlog> queue;
    private final JdbcTemplate jdbcTemplate;
    private final SubRepairTask task;
    private final String baseSql;
    private final AtomicBoolean running;

    private volatile long highWatermark;

    public RepairDataHandler(SubRepairTask task, ConcurrentLinkedQueue<SingleCanalBinlog> queue, AtomicBoolean running) {
        this.queue = queue;
        this.jdbcTemplate = new JdbcTemplate(task.getSourceName());
        this.task = task;
        baseSql = String.format("select %s from %s where %s ",
                task.getColumns(), task.getTableName(), task.getWhere());
        this.running = running;
    }

    @Override
    @SneakyThrows
    public void run() {
        while (running.get()) {
            if (!hasNextBatch()) {
                running.set(false);
                return;
            }
            if (task.getScanMode() == Direct || (queue.size() + BATCH_SIZE) <= MAX_QUEUE_SIZE) {
                List<Map<String, Object>> columnMaps = nextBatch();
                synchronized (running) {
                    for (Map<String, Object> columnMap : columnMaps) {
                        queue.add(new SingleCanalBinlog(task.getSourceName(), task.getTableName(), -1L, CanalEntry.EventType.INSERT, columnMap));
                    }
                    commit();
                }
            } else {
                TimeUnit.MILLISECONDS.sleep(10);
            }
        }
    }

    private boolean hasNextBatch() {
        RepairTask.ScanMode scanMode = task.getScanMode();
        if (scanMode == Direct) {
            return task.getCurrentId() != DIRECT_TASK_FINISH_ID;
        } else {
            return task.getCurrentId() < task.getTargetId();
        }
    }

    private List<Map<String, Object>> nextBatch() {
        String sql = baseSql;
        if (task.getScanMode() == TumblingWindow) {
            highWatermark = Math.min(task.getCurrentId() + BATCH_SIZE, task.getTargetId());
            sql += String.format(" and %s <= id and id < %s", task.getCurrentId(), highWatermark);
        }
        return jdbcTemplate.queryForColumnMaps(sql);
    }

    private void commit() {
        if (task.getScanMode() == Direct) {
            task.setCurrentId(DIRECT_TASK_FINISH_ID);
        } else {
            task.setCurrentId(highWatermark);
        }
    }
}

