package com.liang.common.service.database.holder;

import com.liang.common.service.database.factory.HbaseConnectionFactory;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Connection;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class HbaseConnectionHolder implements IHolder<Connection> {
    private static final Map<String, Connection> pools = new ConcurrentHashMap<>();

    @Override
    public Connection getPool(String name) {
        if (pools.get(name) == null) {
            Connection connection = new HbaseConnectionFactory().createPool(name);
            Connection callback = pools.putIfAbsent(name, connection);
            //说明这次put已经有值了
            if (callback != null) {
                log.warn("putIfAbsent() failed, delete redundant hbaseConnection: {}", name);
                try {
                    connection.close();
                } catch (Exception ignore) {
                }
                connection = null;//help gc
            }
        }
        return pools.get(name);
    }

    @Override
    @SneakyThrows // close()
    public void closeAll() {
        for (Map.Entry<String, Connection> entry : pools.entrySet()) {
            Connection connection = entry.getValue();
            if (!connection.isClosed()) {
                log.warn("hbaseConnection close: {}", entry.getKey());
                connection.close();
            }
        }
    }
}
