package com.liang.repair.test;

import cn.hutool.core.collection.CollUtil;
import com.liang.common.dto.config.RepairTask;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.repair.service.ConfigHolder;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class RepairTest extends ConfigHolder {
    public static void main(String[] args) throws Exception {
        ConcurrentLinkedDeque<Tuple2<Long, Long>> splits = new ConcurrentLinkedDeque<>();
        RepairTask repairTask = new RepairTask();
        repairTask.setSourceName("104.data_bid");
        repairTask.setTableName("company_bid");
        int batchSize = 10000;
        JdbcTemplate jdbcTemplate = new JdbcTemplate(repairTask.getSourceName());
        String sql = new SQL().SELECT("min(id),max(id)")
                .FROM(repairTask.getTableName())
                .toString();
        Tuple2<Long, Long> minAndMax = jdbcTemplate.queryForObject(sql, rs -> Tuple2.of(rs.getLong(1), rs.getLong(2)));
        long min = minAndMax.f0;
//        long max = minAndMax.f1;
        long max = 111111;
        long lastMax = min - 1;
        AtomicBoolean running = new AtomicBoolean(true);
        while (running.get()) {
            String innerSql = new SQL().SELECT("id")
                    .FROM(repairTask.getTableName())
                    .WHERE("id >= " + min)
                    .WHERE("id <= " + max)
                    .WHERE("id > " + lastMax)
                    .ORDER_BY("id")
                    .LIMIT(batchSize)
                    .toString();
            String outerSql = new SQL().SELECT("min(id),max(id),count(1)")
                    .FROM("(" + innerSql + ") t")
                    .toString();
            Tuple3<Long, Long, Long> splitAndCount = jdbcTemplate.queryForObject(outerSql, rs -> Tuple3.of(rs.getLong(1), rs.getLong(2), rs.getLong(3)));
            splits.add(Tuple2.of(splitAndCount.f0, splitAndCount.f1));
            if (splitAndCount.f2 != batchSize) {
                running.set(false);
            } else {
                lastMax = splitAndCount.f1;
            }
        }
        CollUtil.sort(splits, (o1, o2) -> {
            long l = o1.f0 - o2.f0;
            if (l > 0) return 1;
            else if (l < 0) return -1;
            else return 0;
        });
        for (Tuple2<Long, Long> split : splits) {
            System.out.println(split);
        }
    }
}
