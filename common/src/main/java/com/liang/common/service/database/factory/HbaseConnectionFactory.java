package com.liang.common.service.database.factory;

import com.liang.common.dto.config.HbaseConfig;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

@Slf4j
public class HbaseConnectionFactory implements PoolFactory<HbaseConfig, Connection> {

    @Override
    public Connection createPool(String name) {
        return createPool(ConfigUtils.getConfig().getHbaseConfigs().get(name));
    }

    @Override
    public Connection createPool(HbaseConfig config) {
        try {
            Configuration configuration = new Configuration();
            configuration.set("hbase.zookeeper.quorum", config.getZookeeperQuorum());
            Connection connection = ConnectionFactory.createConnection(configuration);
            log.info("HbaseConnectionFactory createPool success, config: {}", JsonUtils.toString(config));
            return connection;
        } catch (Exception e) {
            String msg = "HbaseConnectionFactory createPool error, config: " + JsonUtils.toString(config);
            log.error(msg, e);
            throw new RuntimeException(msg, e);
        }
    }
}
