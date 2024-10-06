package com.liang.flink.basic.repair;

import com.liang.common.dto.config.RepairTask;
import com.liang.common.service.database.template.JdbcTemplate;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.roaringbitmap.longlong.Roaring64Bitmap;

@Slf4j
public class RepairIdGenerator {
    private static final long THRESHOLD = 1000L;

    public static Roaring64Bitmap newIdBitmap(RepairTask repairTask) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(repairTask.getSourceName());

        String queryBoundSql = String.format("SELECT MIN(id), MAX(id) FROM %s", repairTask.getTableName());
        Tuple2<Long, Long> minAndMax = jdbcTemplate.queryForObject(queryBoundSql, rs -> Tuple2.of(rs.getLong(1), rs.getLong(2)));
        Long min = minAndMax.f0;
        Long max = minAndMax.f1;
        log.info(String.format("source: %s, table: %s, min: %,d, max: %,d", repairTask.getSourceName(), repairTask.getTableName(), min, max));

        String queryStatusSql = String.format("SHOW TABLE STATUS LIKE '%s'", repairTask.getTableName());
        Long probablyRows = jdbcTemplate.queryForObject(queryStatusSql, rs -> rs.getLong(5));
        log.info(String.format("probably rows: %,d", probablyRows));

        long mismatch = (max - min) / (probablyRows);
        log.info(String.format("mismatch: %,d", mismatch));

        Roaring64Bitmap bitmap;
        long start = System.currentTimeMillis();
        if (mismatch <= THRESHOLD) {
            log.info("switch to evenly, please waiting for generate id bitmap");
            bitmap = getEvenlyBitmap(min, max);
        } else {
            log.info("switch to unevenly, please waiting for generate id bitmap");
            bitmap = getUnevenlyBitmap(repairTask);
        }
        long end = System.currentTimeMillis();
        log.info("speed {} seconds", (end - start) / 1000);
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
}
