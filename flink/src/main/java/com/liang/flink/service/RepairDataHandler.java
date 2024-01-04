package com.liang.flink.service;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.service.database.template.RedisTemplate;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.dto.SubRepairTask;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.liang.common.dto.config.RepairTask.ScanMode.Direct;
import static com.liang.common.dto.config.RepairTask.ScanMode.TumblingWindow;


@Slf4j
public class RepairDataHandler implements Runnable {
    private final static int BATCH_SIZE = 1024;
    private final static int MAX_QUEUE_SIZE = 10240;
    private final static int WRITE_REDIS_INTERVAL_MILLISECONDS = 1000 * 5;

    private final SubRepairTask task;
    private final AtomicBoolean running;
    private final String repairKey;
    private final String baseSql;
    private final JdbcTemplate jdbcTemplate;
    private final RedisTemplate redisTemplate;

    private long watermark;
    private long lastWriteTimeMillis;

    public RepairDataHandler(SubRepairTask task, AtomicBoolean running, String repairKey) {
        this.task = task;
        this.running = running;
        this.repairKey = repairKey;
        baseSql = String.format("select %s from %s where %s ",
                task.getColumns(), task.getTableName(), task.getWhere());
        jdbcTemplate = new JdbcTemplate(task.getSourceName());
        redisTemplate = new RedisTemplate("metadata");
    }

    @Override
    public void run() {
        ConcurrentLinkedQueue<SingleCanalBinlog> queue = task.getPendingQueue();
        while (hasNextBatch() && running.get()) {
            if (task.getScanMode() == Direct || (queue.size() + BATCH_SIZE) <= MAX_QUEUE_SIZE) {
                List<Map<String, Object>> columnMaps = nextBatch();
                synchronized (running) {
                    for (Map<String, Object> columnMap : columnMaps) {
                        queue.offer(new SingleCanalBinlog(task.getSourceName(), task.getTableName(), -1L, CanalEntry.EventType.INSERT, columnMap, new HashMap<>(), columnMap));
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
        if (currentTimeMillis - lastWriteTimeMillis >= WRITE_REDIS_INTERVAL_MILLISECONDS) {
            long currentId = task.getCurrentId();
            long targetId = task.getTargetId();
            long lag = targetId - currentId;
            redisTemplate.hSet(repairKey, task.getTaskId(),
                    String.format("[running] currentId: %s, targetId: %s, lag: %s", currentId, targetId, lag)
            );
            lastWriteTimeMillis = currentTimeMillis;
        }
    }
}

