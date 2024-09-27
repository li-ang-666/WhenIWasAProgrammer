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

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class RepairSplitEnumerator {
    private static final int BATCH_SIZE = 10000;
    private static final int THREAD_NUM = 10;
    private static final ExecutorService EXECUTOR_SERVICE = Executors.newFixedThreadPool(THREAD_NUM);

    public static void main(String[] args) throws Exception {
        ConfigUtils.setConfig(ConfigUtils.createConfig(null));
        RepairTask repairTask = new RepairTask();
        repairTask.setSourceName("104.data_bid");
        repairTask.setTableName("company_bid");

        long sec1 = System.currentTimeMillis() / 1000;
        Roaring64Bitmap allIds = new RepairSplitEnumerator().getAllIds(repairTask);
        long sec2 = System.currentTimeMillis() / 1000;
    }

    public Roaring64Bitmap getAllIds(RepairTask repairTask) throws Exception {
        Roaring64Bitmap allIds = new Roaring64Bitmap();
        // 查询边界
        JdbcTemplate jdbcTemplate = new JdbcTemplate(repairTask.getSourceName());
        String sql = new SQL().SELECT("MIN(id)", "MAX(id)")
                .FROM(repairTask.getTableName())
                .toString();
        Tuple2<Long, Long> minAndMax = jdbcTemplate.queryForObject(sql, rs -> Tuple2.of(rs.getLong(1), rs.getLong(2)));
        long l = minAndMax.f0;
        long r = minAndMax.f1;
        // 初始化待查询分片队列
        UncheckedSplit firstUncheckedSplit = new UncheckedSplit(l, r);
        Deque<UncheckedSplit> uncheckedSplits = splitUncheckedSplit(firstUncheckedSplit, THREAD_NUM);
        // 开始多线程遍历
        while (!uncheckedSplits.isEmpty()) {
            int size = uncheckedSplits.size();
            AtomicBoolean running = new AtomicBoolean(true);
            CountDownLatch countDownLatch = new CountDownLatch(size);
            List<Future<UncheckedSplit>> futures = new ArrayList<>(size);
            // 发布任务
            for (int i = 0; i < size; i++) {
                SplitTask splitTask = new SplitTask(allIds, repairTask, uncheckedSplits.removeFirst(), running, countDownLatch);
                Future<UncheckedSplit> future = EXECUTOR_SERVICE.submit(splitTask);
                futures.add(future);
            }
            // 等待任务结束
            countDownLatch.await();
            // 补充下一轮待查询分片
            for (Future<UncheckedSplit> future : futures) {
                UncheckedSplit uncheckedSplit = future.get();
                if (uncheckedSplit != null) {
                    uncheckedSplits.addLast(uncheckedSplit);
                }
            }
        }
        return allIds;
    }

    private Deque<UncheckedSplit> splitUncheckedSplit(UncheckedSplit uncheckedSplit, int num) {
        Deque<UncheckedSplit> result = new ConcurrentLinkedDeque<>();
        long l = uncheckedSplit.getL();
        long r = uncheckedSplit.getR();
        long interval = (r - l) / num;
        for (int i = 0; i < num; i++) {
            if (i == num - 1) {
                result.add(new UncheckedSplit(l, r));
            } else {
                result.add(new UncheckedSplit(l, l + interval));
                l = l + interval + 1;
            }
        }
        return result;
    }

    @RequiredArgsConstructor
    private static final class SplitTask implements Callable<UncheckedSplit> {
        private final Roaring64Bitmap all_ids;
        private final RepairTask repairTask;
        private final UncheckedSplit uncheckedSplit;
        private final AtomicBoolean running;
        private final CountDownLatch countDownLatch;

        @Override
        public UncheckedSplit call() {
            JdbcTemplate jdbcTemplate = new JdbcTemplate(repairTask.getSourceName());
            long l = uncheckedSplit.getL();
            long r = uncheckedSplit.getR();
            while (true) {
                String sql = new SQL().SELECT("id")
                        .FROM(repairTask.getTableName())
                        .WHERE("id >= " + l)
                        .WHERE("id <= " + r)
                        .ORDER_BY("id")
                        .LIMIT(BATCH_SIZE)
                        .toString();
                List<Long> res = jdbcTemplate.queryForList(sql, rs -> rs.getLong(1));
                // 如果本线程 [自然] 执行完毕
                if (res.isEmpty()) {
                    running.set(false);
                    countDownLatch.countDown();
                    return null;
                }
                // 收集本批次id, 准备寻找下批次id
                Roaring64Bitmap ids = Roaring64Bitmap.bitmapOf(res.stream().mapToLong(Long::longValue).toArray());
                synchronized (all_ids) {
                    all_ids.or(ids);
                }
                l = ids.last() + 1;
                // 如果本线程 [被动] 执行完毕
                if (!running.get()) {
                    countDownLatch.countDown();
                    // 返回未遍历的切片
                    return new UncheckedSplit(l, r);
                }
            }
        }
    }

    @Data
    @AllArgsConstructor
    private static final class UncheckedSplit {
        private long l;
        private long r;
    }
}
