package com.liang.flink.service;

import com.liang.common.service.Lists;
import com.liang.flink.dao.StreamPositionDao;
import com.liang.flink.dto.StreamSinkInfo;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static com.liang.flink.dao.StreamPositionDao.HEARTBEAT_TIMEOUT;

@Slf4j
public class StreamPositionService {
    private static final Integer SMALL_WARN_THRESHOLD = 3;
    private static final Integer MEDIUM_WARN_THRESHOLD = 5;
    private static final Integer LARGE_WARN_THRESHOLD = 5;

    private static final Integer MAX_KEEP_SECONDS = 5;

    private Map<String, Map<Long, Integer>> orgId2Second2Number = new HashMap<>();

    private StreamPositionDao streamPositionDao = new StreamPositionDao();

    public boolean isDownStream(String orgId, Long updateTime) {
        ArrayList<Boolean> result = new ArrayList<>();
        orgId2Second2Number.compute(orgId, (k_orgId, v_seconds2times) -> {
            //得到或者初始化该Org的小map
            Map<Long, Integer> seconds2times = (v_seconds2times == null ? new HashMap<>() : v_seconds2times);
            //格式化小map的kv
            seconds2times.compute(updateTime, (k_seconds, v_times) -> {
                int times = 1 + (v_times == null ? 0 : v_times);
                if (times >= MEDIUM_WARN_THRESHOLD) {
                    log.warn("单秒峰值: {}, orgId: {}", updateTime + "->" + times, orgId);
                    result.add(true);
                }
                return times;
            });
            //格式化小map的keySet, 删除冗余的时间
            seconds2times.keySet().removeIf(time -> updateTime - time >= MAX_KEEP_SECONDS);
            //计算有效时间内进入flink的数据条数
            val smallWarnElems = Lists.of(seconds2times)
                    .filter(e -> e.f1 >= SMALL_WARN_THRESHOLD)
                    .map(e -> e.f0 + "->" + e.f1)
                    .toList();
            if (smallWarnElems.size() >= LARGE_WARN_THRESHOLD) {
                log.warn("混合峰值: {}, orgId: {}", smallWarnElems, orgId);
                result.add(true);
            }
            return seconds2times;
        });
        return result.contains(true);
    }

    // slot内是单线程的
    // 每个slot只在乎自己分区内所有的key是不是需要回收分区就可以了
    // 不会有线程安全问题, 不需要锁
    public int returnAndCheckPartition(String orgId) {
        int result = -1;
        for (Map.Entry<Integer, StreamSinkInfo> entry : streamPositionDao.getSinkMap().entrySet()) {
            Integer partition = entry.getKey();
            StreamSinkInfo streamSinkInfo = entry.getValue();
            if (streamSinkInfo != null) {
                String o = streamSinkInfo.getOrgId();
                long timeDiff = System.currentTimeMillis() - streamSinkInfo.getLastHeartBeatTime();
                if (o.equals(orgId) && timeDiff < HEARTBEAT_TIMEOUT) {
                    result = partition;
                } else if (orgId2Second2Number.containsKey(o) && timeDiff >= HEARTBEAT_TIMEOUT) {
                    streamPositionDao.returnPartition(partition);
                    orgId2Second2Number.remove(o);
                    log.warn("{} 在下游已经超过 {}秒 没有发送心跳, 拉起, 归还 partition-{}", o, timeDiff / 1000, partition);
                }
            }
        }
        return result;
    }

    // 每个分区一个org的原因: 一个分区多个org, 堵在后面的org无法及时回馈心跳
    // 加锁的原因: 防止前者刚注册, 后者又重新抢注了
    // 每次先看看是不是有空余分区, 再加锁并注册, 防止有公司想要下沉但是没有可用分区的时候, 一直大流量上锁
    public int registerPartition(String orgId) {
        int result = -1;
        boolean in = false;
        for (Map.Entry<Integer, StreamSinkInfo> entry : streamPositionDao.getSinkMap().entrySet()) {
            StreamSinkInfo streamSinkInfo = entry.getValue();
            if (streamSinkInfo == null) {
                in = true;
                break;
            }
        }
        if (in) {
            lock();
            for (Map.Entry<Integer, StreamSinkInfo> entry : streamPositionDao.getSinkMap().entrySet()) {
                Integer partition = entry.getKey();
                StreamSinkInfo streamSinkInfo = entry.getValue();
                if (streamSinkInfo == null) {
                    result = partition;
                    streamPositionDao.registerPartition(partition, orgId);
                    log.warn("{} 被执行下沉到 partition-{}", orgId, partition);
                    break;
                }
            }
            unlock();
        }
        return result;
    }

    // slot内是单线程的
    // 每个slot只在乎自己分区内所有的key是不是需要刷新分区就可以了
    // 不会有线程安全问题, 不需要锁
    public void flushPartition(Object partition, String orgId) {
        streamPositionDao.flushPartition(partition, orgId);
    }

    private boolean tryLock() {
        return "OK".equals(streamPositionDao.tryLock());
    }

    private void lock() {
        while (true) {
            if (tryLock()) break;
        }
    }

    private void unlock() {
        streamPositionDao.unlock();
    }
}
