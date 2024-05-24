package com.liang.flink.basic.repair;

import com.liang.common.service.database.template.RedisTemplate;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.LockSupport;

@Slf4j
@RequiredArgsConstructor
public class RepairReporter implements Runnable {
    private static final int READ_REDIS_INTERVAL_MILLISECONDS = 1000 * 3;
    private final RedisTemplate redisTemplate = new RedisTemplate("metadata");
    private final String repairKey;

    @Override
    public void run() {
        String lastMapString = "{}";
        while (true) {
            LockSupport.parkUntil(System.currentTimeMillis() + READ_REDIS_INTERVAL_MILLISECONDS);
            // 有序
            Map<Integer, String> reportMap = new TreeMap<>();
            for (Map.Entry<String, String> entry : redisTemplate.hScan(repairKey).entrySet()) {
                reportMap.put(Integer.parseInt(entry.getKey()), entry.getValue());
            }
            String mapString = reportMap.toString();
            if (mapString.equals(lastMapString)) {
                continue;
            }
            reportMap.forEach((channel, repairSplitJson) -> {
                log.info("repair report: ");
                log.info("channel: {}, repairSplit: {}", channel, repairSplitJson);
            });
            lastMapString = mapString;
        }
    }
}
