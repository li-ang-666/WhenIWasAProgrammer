package com.liang.flink.job;

import com.alibaba.otter.canal.protocol.FlatMessage;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.cdc.CanalDebeziumDeserializationSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Slf4j
public class CdcJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        env.setParallelism(1);
        MySqlSource<FlatMessage> mySqlSource = MySqlSource.<FlatMessage>builder()
                .hostname("9349c027b3b4414aa5f9019cd218e7a3in01.internal.cn-north-4.mysql.rds.myhuaweicloud.com")
                .port(3306)
                .username("canal_d")
                .password("Canal@Dduan")
                .databaseList(".*")
                .tableList("test.test")
                .serverId("5555")
                .serverTimeZone("Asia/Shanghai")
                .startupOptions(StartupOptions.earliest())
                .deserializer(new CanalDebeziumDeserializationSchema())
                .build();
        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "CdcSource")
                .print();
        env.execute("TestJob");
    }
}
