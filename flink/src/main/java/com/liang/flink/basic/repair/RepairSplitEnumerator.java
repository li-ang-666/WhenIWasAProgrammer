package com.liang.flink.basic.repair;

import com.liang.common.dto.config.RepairTask;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

@Slf4j
public class RepairSplitEnumerator {
    private static final int BATCH_SIZE = 10000;
    private static final int THREAD_NUM = 128;
    private final ExecutorService executorService = Executors.newFixedThreadPool(THREAD_NUM);

    public static void main(String[] args) throws Exception {
        ConfigUtils.setConfig(ConfigUtils.createConfig(null));
        RepairTask repairTask = new RepairTask();
        repairTask.setSourceName("116.prism");
        repairTask.setTableName("equity_ratio");
        @SuppressWarnings("unused")
        Roaring64Bitmap allIds = new RepairSplitEnumerator().getAllIds(repairTask);
    }

    public Roaring64Bitmap getAllIds(RepairTask repairTask) throws Exception {
        long startTime = System.currentTimeMillis();
        Roaring64Bitmap allIds = new Roaring64Bitmap();
        // 查询边界, 初始化待查询分片队列
        JdbcTemplate jdbcTemplate = new JdbcTemplate(repairTask.getSourceName());
        String sql = new SQL().SELECT("MIN(id)", "MAX(id)")
                .FROM(repairTask.getTableName())
                .toString();
        Queue<UncheckedSplit> uncheckedSplits = new ConcurrentLinkedQueue<>(splitUncheckedSplit(jdbcTemplate.queryForObject(sql, rs -> new UncheckedSplit(rs.getLong(1), rs.getLong(2))), THREAD_NUM));
        // 循环处理分片队列
        int times = 0;
        while (!uncheckedSplits.isEmpty()) {
            // 使用优先队列优先切分较大的分片, 尽可能补全分片数到线程数
            PriorityQueue<UncheckedSplit> priorityQueue = new PriorityQueue<>(uncheckedSplits);
            uncheckedSplits.clear();
            while (priorityQueue.size() < THREAD_NUM) {
                @SuppressWarnings("ConstantConditions")
                List<UncheckedSplit> splitedUncheckedSplits = splitUncheckedSplit(priorityQueue.poll(), THREAD_NUM - priorityQueue.size());
                if (splitedUncheckedSplits.size() < (THREAD_NUM - priorityQueue.size())) {
                    break;
                }
                priorityQueue.addAll(splitedUncheckedSplits);
            }
            // 执行任务
            AtomicBoolean running = new AtomicBoolean(true);
            List<SplitTask> tasks = priorityQueue.parallelStream()
                    .map(split -> new SplitTask(uncheckedSplits, allIds, repairTask, split, running))
                    .collect(Collectors.toList());
            executorService.invokeAll(tasks);
            if (++times % 10 == 0) {
                log.info("times: {}, id num: {}", times, String.format("%,d", allIds.getLongCardinality()));
            }
        }
        executorService.shutdown();
        log.info("time: {} seconds, id num: {}", (System.currentTimeMillis() - startTime) / 1000, String.format("%,d", allIds.getLongCardinality()));
        return allIds;
    }

    private List<UncheckedSplit> splitUncheckedSplit(UncheckedSplit uncheckedSplit, int estimatedNum) {
        List<UncheckedSplit> result = new ArrayList<>(estimatedNum);
        long l = uncheckedSplit.getL();
        long r = uncheckedSplit.getR();
        // 无效边界
        if (l > r) {
            return result;
        }
        // 不足以拆分为多个
        else if (r - l + 1 <= BATCH_SIZE) {
            result.add(uncheckedSplit);
            return result;
        }
        // 可以拆分多个, 但不足num个
        else if (r - l + 1 <= (long) estimatedNum * BATCH_SIZE) {
            long interval = BATCH_SIZE - 1;
            while (l <= r) {
                result.add(new UncheckedSplit(l, Math.min(l + interval, r)));
                l = l + interval + 1;
            }
            return result;
        }
        // 可以拆分为num个
        else {
            long interval = ((r - l) / estimatedNum) + 1;
            while (l <= r) {
                result.add(new UncheckedSplit(l, Math.min(l + interval, r)));
                l = l + interval + 1;
            }
            return result;
        }
    }

    @RequiredArgsConstructor
    private static final class SplitTask implements Callable<Void> {
        private final Queue<UncheckedSplit> uncheckedSplits;
        private final Roaring64Bitmap allIds;
        private final RepairTask repairTask;
        private final UncheckedSplit uncheckedSplit;
        private final AtomicBoolean running;

        @Override
        public Void call() {
            JdbcTemplate jdbcTemplate = new JdbcTemplate(repairTask.getSourceName());
            long l = uncheckedSplit.getL();
            long r = uncheckedSplit.getR();
            while (true) {
                List<Long> ids;
                if (l > r) {
                    ids = new ArrayList<>();
                } else {
                    String sql = new SQL().SELECT("id")
                            .FROM(repairTask.getTableName())
                            .WHERE("id >= " + l)
                            .WHERE("id <= " + r)
                            .ORDER_BY("id ASC")
                            .LIMIT(BATCH_SIZE)
                            .toString();
                    ids = jdbcTemplate.queryForList(sql, rs -> rs.getLong(1));
                }
                // 如果本线程 [自然] 执行完毕
                if (ids.isEmpty()) {
                    running.set(false);
                    return null;
                }
                // 收集本批次id, 准备寻找下批次id
                synchronized (allIds) {
                    allIds.add(ids.parallelStream().mapToLong(e -> e).toArray());
                }
                l = ids.get(ids.size() - 1) + 1;
                // 如果本线程 [被动] 执行完毕
                if (!running.get()) {
                    // 返还未处理分片
                    if (l <= r) {
                        uncheckedSplits.add(new UncheckedSplit(l, r));
                    }
                    return null;
                }
            }
        }
    }

    @Data
    @AllArgsConstructor
    private static final class UncheckedSplit implements Comparable<UncheckedSplit> {
        private long l;
        private long r;

        @Override
        public int compareTo(@NotNull UncheckedSplit another) {
            long diff = this.r - this.l;
            long anotherDiff = another.r - another.l;
            if (anotherDiff - diff > 0) {
                return 1;
            } else if (anotherDiff - diff < 0) {
                return -1;
            } else {
                return 0;
            }
        }
    }
}
