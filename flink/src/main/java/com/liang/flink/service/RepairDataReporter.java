package com.liang.flink.service;

import com.liang.common.service.database.template.RedisTemplate;
import com.liang.common.util.JsonUtils;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
@RequiredArgsConstructor
public class RepairDataReporter implements Runnable {
    private static final int READ_REDIS_INTERVAL_SECONDS = 60;
    private final RedisTemplate redisTemplate = new RedisTemplate("metadata");
    private final String repairId;

    @Override
    @SneakyThrows(InterruptedException.class)
    public void run() {
        while (true) {
            TimeUnit.SECONDS.sleep(READ_REDIS_INTERVAL_SECONDS);
            Map<String, String> reportMap = redisTemplate.hScan(repairId);
            String reportContent = JsonUtils.toString(reportMap);
            log.info("repair report: {}", reportContent);
        }
    }
}
