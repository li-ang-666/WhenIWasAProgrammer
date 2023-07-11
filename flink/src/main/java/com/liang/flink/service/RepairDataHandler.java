package com.liang.flink.service;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.dto.SubRepairTask;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.liang.common.dto.config.RepairTask.ScanMode.Direct;
import static com.liang.common.dto.config.RepairTask.ScanMode.TumblingWindow;


@Slf4j
public class RepairDataHandler implements Runnable {
    private final static int BATCH_SIZE = 1024;
    private final static int MAX_QUEUE_SIZE = 102400;
    private final static int REPORT_INTERVAL = 1000 * 60;

    private final SubRepairTask task;
    private final AtomicBoolean running;
    private final String baseSql;
    private final JdbcTemplate jdbcTemplate;

    private volatile long watermark;
    private long lastReportTimeMillis;

    public RepairDataHandler(SubRepairTask task, AtomicBoolean running) {
        this.task = task;
        this.running = running;
        baseSql = String.format("select %s from %s where %s ",
                task.getColumns(), task.getTableName(), task.getWhere());
        jdbcTemplate = new JdbcTemplate(task.getSourceName());
    }

    @Override
    public void run() {
        ConcurrentLinkedQueue<SingleCanalBinlog> queue = task.getPendingQueue();
        while (hasNextBatch() && running.get()) {
            if (task.getScanMode() == Direct || (queue.size() + BATCH_SIZE) <= MAX_QUEUE_SIZE) {
                List<Map<String, Object>> columnMaps = nextBatch();
                synchronized (task) {
                    for (Map<String, Object> columnMap : columnMaps) {
                        queue.offer(new SingleCanalBinlog(task.getSourceName(), task.getTableName(), -1L, CanalEntry.EventType.INSERT, columnMap));
                    }
                    commit();
                }
            }
        }
        running.set(false);
    }

    private boolean hasNextBatch() {
        return task.getCurrentId() < task.getTargetId();
    }

    private List<Map<String, Object>> nextBatch() {
        StringBuilder sqlBuilder = new StringBuilder(baseSql);
        if (task.getScanMode() == TumblingWindow) {
            watermark = Math.min(task.getCurrentId() + BATCH_SIZE, task.getTargetId());
            sqlBuilder.append(String.format(" and %s <= id and id < %s", task.getCurrentId(), watermark));
        }
        return jdbcTemplate.queryForColumnMaps(sqlBuilder.toString());
    }

    private void commit() {
        task.setCurrentId(task.getScanMode() == Direct ? 1 : watermark);
        report();
    }

    private void report() {
        long currentTimeMillis = System.currentTimeMillis();
        if (currentTimeMillis - lastReportTimeMillis >= REPORT_INTERVAL) {
            String id = task.getTaskId();
            long currentId = task.getCurrentId();
            long targetId = task.getTargetId();
            log.info("sender report, task-{}: currentId {}, targetId {}, lag {}", currentId, targetId, targetId - currentId);
            lastReportTimeMillis = currentTimeMillis;
        }
    }
}

