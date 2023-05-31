package com.liang.common.service.database.factory;

import com.liang.common.dto.config.HbaseDbConfig;
import com.liang.common.util.ConfigUtils;
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
    public static Connection create(String name) {
        HbaseDbConfig config = ConfigUtils.getConfig().getHbaseDbConfigs().get(name);
        Configuration conf = new Configuration();
        String zookeeperQuorum = config.getZookeeperQuorum();
        conf.set("hbase.zookeeper.quorum", zookeeperQuorum);
        log.info("hbase连接加载: {}", config);
        return ConnectionFactory.createConnection(conf);
    }
}
