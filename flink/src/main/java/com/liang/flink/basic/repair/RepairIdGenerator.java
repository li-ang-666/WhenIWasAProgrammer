package com.liang.flink.basic.repair;

import cn.hutool.core.util.StrUtil;
import com.liang.common.dto.config.RepairTask;
import com.liang.common.service.database.template.JdbcTemplate;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@SuppressWarnings("StatementWithEmptyBody")
public class RepairIdGenerator {
    private final ExecutorService executorService = Executors.newFixedThreadPool(2);

    public Roaring64Bitmap getAllIds(RepairTask repairTask) {
        long startTime = System.currentTimeMillis();
        // 查询用到的索引
        Roaring64Bitmap allIds = new Roaring64Bitmap();
        JdbcTemplate jdbcTemplate = new JdbcTemplate(repairTask.getSourceName());
        String indexName = jdbcTemplate.queryForObject(String.format("EXPLAIN SELECT id FROM %s", repairTask.getTableName()), rs -> rs.getString(7));
        String orderByColumnName = jdbcTemplate.queryForObject(String.format("SHOW INDEXES FROM %s WHERE KEY_NAME = '%s' AND SEQ_IN_INDEX = 1", repairTask.getTableName(), indexName), rs -> rs.getString(5));
        // 执行双向流式查询
        String uuid1 = UUID.randomUUID().toString();
        String uuid2 = UUID.randomUUID().toString();
        AtomicBoolean running = new AtomicBoolean(true);
        executorService.submit(new StreamQueryTask(repairTask, uuid1, orderByColumnName + " ASC", allIds, running));
        executorService.submit(new StreamQueryTask(repairTask, uuid2, orderByColumnName + " DESC", allIds, running));
        // 等待流式查询结束
        while (!running.get()) {
        }
        // kill session
        List<Tuple2<String, String>> sessionIdAndSqls = jdbcTemplate.queryForList("SHOW PROCESSLIST", rs -> Tuple2.of(rs.getString(1), rs.getString(8)));
        for (Tuple2<String, String> sessionIdAndSql : sessionIdAndSqls) {
            String sessionId = sessionIdAndSql.f0;
            String sql = sessionIdAndSql.f1;
            if (StrUtil.containsAny(sql, uuid1, uuid2)) {
                jdbcTemplate.update("KILL " + sessionId);
            }
        }
        executorService.shutdown();
        log.info("time: {} seconds, id num: {}", (System.currentTimeMillis() - startTime) / 1000, String.format("%,d", allIds.getLongCardinality()));
        return allIds;
    }

    @Slf4j
    @RequiredArgsConstructor
    private static final class StreamQueryTask implements Callable<Void> {
        private final RepairTask repairTask;
        private final String uuid;
        private final String orderBySyntax;
        private final Roaring64Bitmap allIds;
        private final AtomicBoolean running;

        @Override
        public Void call() {
            JdbcTemplate jdbcTemplate = new JdbcTemplate(repairTask.getSourceName());
            String sql = String.format("/* %s */SELECT id FROM %s ORDER BY %s", uuid, repairTask.getTableName(), orderBySyntax);
            try {
                jdbcTemplate.streamQuery(true, sql, rs -> {
                    if (running.get()) {
                        long id = rs.getLong(1);
                        synchronized (allIds) {
                            if (running.get() && !allIds.contains(id)) {
                                allIds.add(id);
                            } else {
                                running.set(false);
                            }
                        }
                    }
                });
            } catch (Exception ignore) {
            }
            return null;
        }
    }
}
