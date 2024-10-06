package com.liang.flink.basic.repair;

import com.liang.common.dto.config.RepairTask;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.service.database.template.RedisTemplate;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.roaringbitmap.longlong.Roaring64Bitmap;

@Slf4j
public class RepairIdGenerator {
    private static final long THRESHOLD = 1000L;

    public static Roaring64Bitmap newIdBitmap(RepairTask repairTask, String repairReportKey) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(repairTask.getSourceName());
        // 边界
        String queryBoundSql = String.format("SELECT MIN(id), MAX(id) FROM %s", repairTask.getTableName());
        Tuple2<Long, Long> minAndMax = jdbcTemplate.queryForObject(queryBoundSql, rs -> Tuple2.of(rs.getLong(1), rs.getLong(2)));
        Long min = minAndMax.f0;
        Long max = minAndMax.f1;
        report(String.format("source: %s, table: %s, min: %,d, max: %,d", repairTask.getSourceName(), repairTask.getTableName(), min, max), repairReportKey);
        // 粗略行数
        String queryStatusSql = String.format("SHOW TABLE STATUS LIKE '%s'", repairTask.getTableName());
        Long probablyRows = jdbcTemplate.queryForObject(queryStatusSql, rs -> rs.getLong(5));
        report(String.format("probably rows: %,d", probablyRows), repairReportKey);
        // 偏差
        long mismatch = (max - min) / (probablyRows);
        report(String.format("mismatch: %,d", mismatch), repairReportKey);
        // 生成
        Roaring64Bitmap bitmap;
        long start = System.currentTimeMillis();
        if (mismatch <= THRESHOLD) {
            report("switch to evenly, please waiting for generate id bitmap", repairReportKey);
            bitmap = getEvenlyBitmap(min, max);
        } else {
            report("switch to unevenly, please waiting for generate id bitmap", repairReportKey);
            bitmap = getUnevenlyBitmap(repairTask);
        }
        long end = System.currentTimeMillis();
        report(String.format("speed %s seconds", (end - start) / 1000), repairReportKey);
        return bitmap;
    }

    private static Roaring64Bitmap getEvenlyBitmap(long min, long max) {
        Roaring64Bitmap bitmap = new Roaring64Bitmap();
        for (long i = min; i <= max; i++) {
            bitmap.add(i);
        }
        return bitmap;
    }

    private static Roaring64Bitmap getUnevenlyBitmap(RepairTask repairTask) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(repairTask.getSourceName());
        String sql = String.format("SELECT id FROM %s", repairTask.getTableName());
        Roaring64Bitmap bitmap = new Roaring64Bitmap();
        jdbcTemplate.streamQuery(true, sql, rs -> bitmap.add(rs.getLong(1)));
        return bitmap;
    }

    private static void report(String logs, String repairReportKey) {
        logs = String.format("[_RepairSource_] %s", logs);
        new RedisTemplate("metadata").rPush(repairReportKey, logs);
    }
}
