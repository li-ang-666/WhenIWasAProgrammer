package com.liang.common.service.database.holder;

import com.liang.common.service.database.factory.HbaseConnectionFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Connection;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class HbaseConnectionHolder implements MultiPoolHolder<Connection> {
    private static final Map<String, Connection> POOLS = new ConcurrentHashMap<>();

    @Override
    public Connection getPool(String name) {
        return POOLS.computeIfAbsent(name, k ->
                new HbaseConnectionFactory()
                        .createPool(name)
        );
    }

    @Override
    public void closeAll() {
        POOLS.forEach((name, pool) -> {
            try {
                if (!pool.isClosed()) {
                    log.warn("hbase close: {}", name);
                    pool.close();
                }
            } catch (Exception ignore) {
            }
        });
    }
}
