package com.liang.flink.job;

import com.liang.flink.basic.StreamEnvironmentFactory;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class CdcJob {
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            args = new String[]{"cdc.yml"};
        }
        StreamExecutionEnvironment env = StreamEnvironmentFactory.create(args);
        Properties properties = new Properties();
        properties.setProperty("bigint.unsigned.handling.mode", "long");
        properties.setProperty("decimal.handling.mode", "double");

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("140f5010b84c4d9abfc4d13bc597a2ebin01.internal.cn-north-4.mysql.rds.myhuaweicloud.com")
                .port(3306)
                .databaseList(".*")
                .tableList(".*?itch_point_secondary_dims_uv_count")
                .username("jdhw_d_data_dml")
                .password("2s0^tFa4SLrp72")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .debeziumProperties(new Properties() {{
                    put("bigint.unsigned.handling.mode", "long");
                    put("decimal.handling.modee", "double");
                }})
                .jdbcProperties(new Properties() {{
                    put("jdbc.properties.serverTimezone", "GMT%2B8");
                    put("jdbc.properties.zeroDateTimeBehavior", "convertToNull");
                    put("jdbc.properties.useUnicode", "true");
                    put("jdbc.properties.characterEncoding", "UTF-8");
                    put("jdbc.properties.characterSetResults", "UTF-8");
                    put("jdbc.properties.useSSL", "false");
                }})
                .build();
        env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "CdcSource")
                .setParallelism(1)
                .print().setParallelism(1);

        env.execute("Print MySQL Snapshot + Binlog");
    }
}
