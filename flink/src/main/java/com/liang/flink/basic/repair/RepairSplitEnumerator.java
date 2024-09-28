package com.liang.flink.basic.repair;

import com.liang.common.dto.config.RepairTask;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class RepairSplitEnumerator {
    private static final int BATCH_SIZE = 10000;
    private static final int THREAD_NUM = 128;

    public static void main(String[] args) throws Exception {
        ConfigUtils.setConfig(ConfigUtils.createConfig(null));
        RepairTask repairTask = new RepairTask();
        repairTask.setSourceName("116.prism");
        repairTask.setTableName("equity_ratio");

        long sec1 = System.currentTimeMillis() / 1000;
        Roaring64Bitmap allIds = new RepairSplitEnumerator().getAllIds(repairTask);
        long sec2 = System.currentTimeMillis() / 1000;
        log.info("time: {} seconds, id num: {}", sec2 - sec1, allIds.getLongCardinality());
    }

    public Roaring64Bitmap getAllIds(RepairTask repairTask) throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_NUM);
        Roaring64Bitmap allIds = new Roaring64Bitmap();
        // 查询边界
        JdbcTemplate jdbcTemplate = new JdbcTemplate(repairTask.getSourceName());
        String sql = new SQL().SELECT("MIN(id)", "MAX(id)")
                .FROM(repairTask.getTableName())
                .toString();
        Tuple2<Long, Long> minAndMax = jdbcTemplate.queryForObject(sql, rs -> Tuple2.of(rs.getLong(1), rs.getLong(2)));
        long l = minAndMax.f0;
        long r = minAndMax.f1;
        // 首次初始化待查询分片队列
        UncheckedSplit firstUncheckedSplit = new UncheckedSplit(l, r);
        Deque<UncheckedSplit> uncheckedSplits = splitUncheckedSplit(firstUncheckedSplit, THREAD_NUM);
        // 开始多线程遍历
        while (!uncheckedSplits.isEmpty()) {
            // 分片不足线程数, 则补充(有可能补充不到)
            // 记录一下初始分片数
            int num = uncheckedSplits.size();
            while (num-- > 0 || uncheckedSplits.size() < THREAD_NUM) {
                UncheckedSplit uncheckedSplit = uncheckedSplits.removeFirst();
                uncheckedSplits.addAll(splitUncheckedSplit(uncheckedSplit, THREAD_NUM - uncheckedSplits.size()));
            }
            // 分片补充后, 重新记录一下size
            int size = uncheckedSplits.size();
            AtomicBoolean running = new AtomicBoolean(true);
            CountDownLatch countDownLatch = new CountDownLatch(size);
            // 发布任务
            for (int i = 0; i < size; i++) {
                UncheckedSplit uncheckedSplit = uncheckedSplits.removeFirst();
                SplitTask splitTask = new SplitTask(uncheckedSplits, allIds, repairTask, uncheckedSplit, running, countDownLatch);
                executorService.execute(splitTask);
            }
            // 等待任务结束
            countDownLatch.await();
            log.info("id num: {}", allIds.getLongCardinality());
        }
        executorService.shutdown();
        return allIds;
    }

    private Deque<UncheckedSplit> splitUncheckedSplit(UncheckedSplit uncheckedSplit, long num) {
        Deque<UncheckedSplit> result = new ConcurrentLinkedDeque<>();
        long l = uncheckedSplit.getL();
        long r = uncheckedSplit.getR();
        // 无效边界
        if (l > r) {
            return result;
        }
        // 不足以拆分为多个
        else if (r - l + 1 <= BATCH_SIZE) {
            result.addLast(uncheckedSplit);
            return result;
        }
        // 可以拆分多个, 但不足num个
        else if (r - l + 1 <= num * BATCH_SIZE) {
            long interval = BATCH_SIZE - 1;
            while (l <= r) {
                result.addLast(new UncheckedSplit(l, Math.min(l + interval, r)));
                l = l + interval + 1;
            }
            return result;
        }
        // 可以拆分为num个
        else {
            long interval = ((r - l) / num) + 1;
            while (l <= r) {
                result.addLast(new UncheckedSplit(l, Math.min(l + interval, r)));
                l = l + interval + 1;
            }
            return result;
        }
    }

    @RequiredArgsConstructor
    private static final class SplitTask implements Runnable {
        private final Deque<UncheckedSplit> uncheckedSplits;
        private final Roaring64Bitmap allIds;
        private final RepairTask repairTask;
        private final UncheckedSplit uncheckedSplit;
        private final AtomicBoolean running;
        private final CountDownLatch countDownLatch;

        @Override
        public void run() {
            JdbcTemplate jdbcTemplate = new JdbcTemplate(repairTask.getSourceName());
            long l = uncheckedSplit.getL();
            long r = uncheckedSplit.getR();
            while (true) {
                String sql = new SQL().SELECT("id")
                        .FROM(repairTask.getTableName())
                        .WHERE("id >= " + l)
                        .WHERE("id <= " + r)
                        .ORDER_BY("id ASC")
                        .LIMIT(BATCH_SIZE)
                        .toString();
                List<Long> res = jdbcTemplate.queryForList(sql, rs -> rs.getLong(1));
                // 如果本线程 [自然] 执行完毕
                if (res.isEmpty()) {
                    running.set(false);
                    break;
                }
                // 收集本批次id, 准备寻找下批次id
                Roaring64Bitmap ids = Roaring64Bitmap.bitmapOf(res.stream().mapToLong(Long::longValue).toArray());
                synchronized (allIds) {
                    allIds.or(ids);
                }
                l = ids.last() + 1;
                // 如果本线程 [被动] 执行完毕
                if (!running.get()) {
                    // 补充未处理分片
                    if (l <= r) {
                        uncheckedSplits.addLast(new UncheckedSplit(l, r));
                    }
                    break;
                }
            }
            countDownLatch.countDown();
        }
    }

    @Data
    @AllArgsConstructor
    private static final class UncheckedSplit {
        private long l;
        private long r;
    }
}
