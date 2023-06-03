package com.liang.common.service.database.factory;

import com.liang.common.dto.config.HbaseDbConfig;
import com.liang.common.util.ConfigUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

@Slf4j
public class HbaseConnectionFactory implements IFactory<Connection> {

    @Override
    @SneakyThrows
    public Connection createPool(String name) {
        HbaseDbConfig config = ConfigUtils.getConfig().getHbaseDbConfigs().get(name);
        Configuration configuration = new Configuration();
        configuration.set("hbase.zookeeper.quorum", config.getZookeeperQuorum());
        log.info("hbaseConnection 加载: {}", config);
        return ConnectionFactory.createConnection(configuration);
    }
}
