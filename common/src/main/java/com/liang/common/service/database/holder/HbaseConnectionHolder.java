package com.liang.common.service.database.holder;

import com.liang.common.service.database.factory.HbaseConnectionFactory;
import org.apache.hadoop.hbase.client.Connection;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
                connection = null;//help gc
            }
        }
        return HbaseConnections.get(name);
    }
}
