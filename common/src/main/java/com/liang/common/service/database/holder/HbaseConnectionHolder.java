package com.liang.common.service.database.holder;

import com.liang.common.service.database.factory.HbaseConnectionFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Connection;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class HbaseConnectionHolder {
    private static final Map<String, Connection> HbaseConnections = new ConcurrentHashMap<>();

    private HbaseConnectionHolder() {
    }

    public static Connection getConnection(String name) {
        if (HbaseConnections.get(name) == null) {
            Connection connection = HbaseConnectionFactory.create(name);
            Connection callback = HbaseConnections.putIfAbsent(name, connection);
            //说明这次put已经有值了
            if (callback != null) {
                log.warn("putIfAbsent() fail, delete redundant hbaseConnection: {}", name);
                try {
                    connection.close();
                } catch (Exception ignore) {
                }
                connection = null;//help gc
            }
        }
        return HbaseConnections.get(name);
    }
}
