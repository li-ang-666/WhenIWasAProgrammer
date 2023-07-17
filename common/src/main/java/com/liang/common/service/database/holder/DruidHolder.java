package com.liang.common.service.database.holder;

import com.alibaba.druid.pool.DruidDataSource;
import com.liang.common.service.database.factory.DruidFactory;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class DruidHolder implements IHolder<DruidDataSource> {
    private static final Map<String, DruidDataSource> pools = new ConcurrentHashMap<>();

    @Override
    public DruidDataSource getPool(String name) {
        if (pools.get(name) == null) {
            DruidDataSource druidDataSource = new DruidFactory().createPool(name);
            DruidDataSource callback = pools.putIfAbsent(name, druidDataSource);
            //说明这次put已经有值了
            if (callback != null) {
                log.warn("putIfAbsent() failed, delete redundant druid: {}", name);
                try {
                    druidDataSource.close();
                } catch (Exception ignore) {
                }
                druidDataSource = null;//help gc
            }
        }
        return pools.get(name);
    }

    @Override
    public void closeAll() {
        for (Map.Entry<String, DruidDataSource> entry : pools.entrySet()) {
            DruidDataSource dataSource = entry.getValue();
            if (!dataSource.isClosed()) {
                log.warn("druid close: {}", entry.getKey());
                dataSource.close();
            }
        }
    }
}