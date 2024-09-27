package com.liang.repair.test;

import com.liang.common.dto.config.RepairTask;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.repair.service.ConfigHolder;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class RepairTest extends ConfigHolder {
    private static final RepairTask REPAIR_TASK = new RepairTask();
    private static final int BATCH_SIZE = 10000;
    private static final Roaring64Bitmap ID_BITMAP = new Roaring64Bitmap();
    private static final Deque<UncheckedSplit> UNCHECKED_SPLITS = new ConcurrentLinkedDeque<>();
    private static final ExecutorService EXECUTOR_SERVICE = Executors.newFixedThreadPool(10);

    static {
        REPAIR_TASK.setSourceName("104.data_bid");
        REPAIR_TASK.setTableName("company_bid");
        UNCHECKED_SPLITS.add(new UncheckedSplit(1, 1111111));
        UNCHECKED_SPLITS.add(new UncheckedSplit(1111112, 2222222));
        UNCHECKED_SPLITS.add(new UncheckedSplit(2222223, 3333333));
        UNCHECKED_SPLITS.add(new UncheckedSplit(3333334, 4444444));
        UNCHECKED_SPLITS.add(new UncheckedSplit(4444445, 5555555));
        UNCHECKED_SPLITS.add(new UncheckedSplit(5555556, 6666666));
        UNCHECKED_SPLITS.add(new UncheckedSplit(6666667, 7777777));
        UNCHECKED_SPLITS.add(new UncheckedSplit(7777778, 8888888));
        UNCHECKED_SPLITS.add(new UncheckedSplit(8888889, 9999999));
        UNCHECKED_SPLITS.add(new UncheckedSplit(10000000, 11111111));
    }

    public static void main(String[] args) throws Exception {
        long sec1 = System.currentTimeMillis() / 1000;
        while (!UNCHECKED_SPLITS.isEmpty()) {
            int size = UNCHECKED_SPLITS.size();
            log.info("size: {}", size);
            AtomicBoolean running = new AtomicBoolean(true);
            CountDownLatch countDownLatch = new CountDownLatch(size);
            List<Future<UncheckedSplit>> futures = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                Future<UncheckedSplit> future = EXECUTOR_SERVICE.submit(new SplitTask(UNCHECKED_SPLITS.removeFirst(), running, countDownLatch));
                futures.add(future);
            }
            countDownLatch.await();
            for (Future<UncheckedSplit> future : futures) {
                UncheckedSplit uncheckedSplit = future.get();
                if (uncheckedSplit != null) {
                    UNCHECKED_SPLITS.addLast(uncheckedSplit);
                }
            }
        }
        EXECUTOR_SERVICE.shutdown();
        long sec2 = System.currentTimeMillis() / 1000;
        log.info("sec: {}, bitmap size: {}", sec2 - sec1, ID_BITMAP.getLongCardinality());
    }

    @RequiredArgsConstructor
    private static final class SplitTask implements Callable<UncheckedSplit> {
        private final UncheckedSplit uncheckedSplit;
        private final AtomicBoolean running;
        private final CountDownLatch countDownLatch;

        @Override
        public UncheckedSplit call() {
            JdbcTemplate jdbcTemplate = new JdbcTemplate(REPAIR_TASK.getSourceName());
            long l = uncheckedSplit.getL();
            long r = uncheckedSplit.getR();
            while (true) {
                String sql = new SQL().SELECT("id")
                        .FROM(REPAIR_TASK.getTableName())
                        .WHERE("id >= " + l)
                        .WHERE("id <= " + r)
                        .ORDER_BY("id")
                        .LIMIT(BATCH_SIZE)
                        .toString();
                List<Long> ids = jdbcTemplate.queryForList(sql, rs -> rs.getLong(1));
                // 如果本线程 [自然] 执行完毕
                if (ids.isEmpty()) {
                    running.set(false);
                    countDownLatch.countDown();
                    return null;
                }
                // 收集本批次id, 准备寻找下批次id
                Roaring64Bitmap id_bitmap = Roaring64Bitmap.bitmapOf(ids.stream().mapToLong(Long::longValue).toArray());
                synchronized (ID_BITMAP) {
                    ID_BITMAP.or(id_bitmap);
                }
                l = id_bitmap.last() + 1;
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
