package com.liang.flink.basic.repair;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.service.database.template.RedisTemplate;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.dto.SubRepairTask;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.liang.common.dto.config.RepairTask.ScanMode.Direct;
import static com.liang.common.dto.config.RepairTask.ScanMode.TumblingWindow;


@Slf4j
@RequiredArgsConstructor
public class RepairDataHandler implements Runnable, Iterator<List<Map<String, Object>>> {
    private static final int QUERY_BATCH_SIZE = 1024;
    private static final int MAX_QUEUE_SIZE = 10240;
    private static final int WRITE_REDIS_INTERVAL_MILLISECONDS = 1000 * 5;
    private static final int DIRECT_SCAN_COMPLETE_FLAG = 1;
    private final SubRepairTask task;
    private final AtomicBoolean running;
    private final String repairKey;
    private String baseSql;
    private JdbcTemplate jdbcTemplate;
    private RedisTemplate redisTemplate;
    private long lastWriteTimeMillis;

    @Override
    public void run() {
        open();
        ConcurrentLinkedQueue<SingleCanalBinlog> queue = task.getPendingQueue();
        while (hasNext() && running.get()) {
            if (queue.size() + QUERY_BATCH_SIZE > MAX_QUEUE_SIZE) continue;
            List<Map<String, Object>> columnMaps = next();
            synchronized (repairKey) {
                for (Map<String, Object> columnMap : columnMaps) {
                    queue.offer(new SingleCanalBinlog(task.getSourceName(), task.getTableName(), -1L, CanalEntry.EventType.INSERT, columnMap, new HashMap<>(), columnMap));
                }
                commit();
            }
        }
        running.set(false);
    }

    public void open() {
        baseSql = String.format("select %s from %s where %s", task.getColumns(), task.getTableName(), task.getWhere());
        jdbcTemplate = new JdbcTemplate(task.getSourceName());
        redisTemplate = new RedisTemplate("metadata");
    }

    /**
     * `currentId` is unprocessed
     */
    @Override
    public boolean hasNext() {
        return task.getScanMode() == Direct ?
                task.getCurrentId() != DIRECT_SCAN_COMPLETE_FLAG : task.getCurrentId() <= task.getTargetId();
    }

    @Override
    public List<Map<String, Object>> next() {
        StringBuilder sqlBuilder = new StringBuilder(baseSql);
        if (task.getScanMode() == TumblingWindow) {
            sqlBuilder.append(String.format(" and %s <= id and id < %s", task.getCurrentId(), task.getCurrentId() + QUERY_BATCH_SIZE));
        }
        return jdbcTemplate.queryForColumnMaps(sqlBuilder.toString());
    }

    private void commit() {
        task.setCurrentId(task.getScanMode() == Direct ? DIRECT_SCAN_COMPLETE_FLAG : task.getCurrentId() + QUERY_BATCH_SIZE);
        long currentTimeMillis = System.currentTimeMillis();
        if (currentTimeMillis - lastWriteTimeMillis >= WRITE_REDIS_INTERVAL_MILLISECONDS) {
            String info = String.format("[running] currentId: %s, targetId: %s, lag: %s", task.getCurrentId(), task.getTargetId(), task.getTargetId() - task.getCurrentId());
            redisTemplate.hSet(repairKey, task.getTaskId(), info);
            lastWriteTimeMillis = currentTimeMillis;
        }
    }
}

