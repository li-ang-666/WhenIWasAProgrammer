package com.liang.common.service.database.holder;

import com.liang.common.dto.config.HbaseConfig;
import com.liang.common.service.database.factory.HbaseConnectionFactory;
import com.liang.common.util.ConfigUtils;
import org.apache.hadoop.hbase.client.Connection;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HbaseConnectionHolder {
    private HbaseConnectionHolder() {
    }

    private static final Map<String, Connection> HbaseConnections = new ConcurrentHashMap<>();

    public static Connection getConnection(String name) {
        if (HbaseConnections.get(name) == null) {
            HbaseConfig hbaseConfig = ConfigUtils.getConfig().getHbaseConfigs().get(name);
            Connection connection = HbaseConnectionFactory.create(hbaseConfig);
            Connection callback = HbaseConnections.putIfAbsent(name, connection);
            //说明这次put已经有值了
            if (callback != null) {
                connection = null;//help gc
            }
        }
        return HbaseConnections.get(name);
    }
}
