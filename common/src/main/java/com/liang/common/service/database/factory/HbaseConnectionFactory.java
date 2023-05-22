package com.liang.common.service.database.factory;

import com.liang.common.dto.config.HbaseConfig;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

@Slf4j
public class HbaseConnectionFactory {
    private HbaseConnectionFactory() {
    }

    @SneakyThrows
    public static Connection create(HbaseConfig config) {
        Configuration conf = new Configuration();
        String zookeeperQuorum = config.getZookeeperQuorum();
        conf.set("hbase.zookeeper.quorum", zookeeperQuorum);
        return ConnectionFactory.createConnection(conf);
    }
}
