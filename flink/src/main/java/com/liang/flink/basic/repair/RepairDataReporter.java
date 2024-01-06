package com.liang.flink.basic.repair;

import com.liang.common.service.database.template.RedisTemplate;
import com.liang.common.util.JsonUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.locks.LockSupport;

@Slf4j
@RequiredArgsConstructor
public class RepairDataReporter implements Runnable {
    private static final int READ_REDIS_INTERVAL_MILLISECONDS = 1000 * 3;
    private final RedisTemplate redisTemplate = new RedisTemplate("metadata");
    private final String repairKey;

    @Override
    public void run() {
        while (true) {
            LockSupport.parkUntil(System.currentTimeMillis() + READ_REDIS_INTERVAL_MILLISECONDS);
            Map<String, String> reportMap = redisTemplate.hScan(repairKey);
            String reportContent = JsonUtils.toString(reportMap);
            log.info("repair report: {}", reportContent);
        }
    }
}
