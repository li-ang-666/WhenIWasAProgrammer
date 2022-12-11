package com.liang.repair.launch;

import com.liang.common.database.cluster.JdbcPoolCluster;
import com.liang.common.database.cluster.JedisPoolCluster;
import com.liang.common.dto.config.ConnectionConfig;
import com.liang.repair.trait.Runner;
import com.liang.common.util.GlobalUtils;
import com.liang.common.util.YamlUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;

@Slf4j
public class Launcher {
    public static void main(String[] args) {
        try {
            ConnectionConfig connectionConfig = YamlUtils.fromResource("connections.yml", ConnectionConfig.class);
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            GlobalUtils.init(connectionConfig, parameterTool);

            Object instance = Class.forName("com.liang.repair.impl." + parameterTool.get("impl")).newInstance();
            ((Runner) instance).run(args);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            JdbcPoolCluster.release();
            JedisPoolCluster.release();
        }
    }
}
