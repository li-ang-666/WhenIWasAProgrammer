package com.liang.flink.job;

import com.liang.common.util.JsonUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.cdc.MapDebeziumDeserializationSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;

@Slf4j
public class CdcTestJob {
    private static final String CDC_HOSTNAME = "ee59dd05fc0f4bb9a2497c8d9146a53cin01.internal.cn-north-4.mysql.rds.myhuaweicloud.com";
    private static final String CDC_DATABASE = "company_base";
    private static final String CDC_TABLE = "company_index";
    private static final int CDC_PORT = 3306;
    private static final String CDC_USERNAME = "jdhw_d_data_dml";
    private static final String CDC_PASSWORD = "2s0^tFa4SLrp72";
    private static final String CDC_SERVER_ID = "6000-6100";
    private static final String CDC_TIMEZONE = "Asia/Shanghai";
    private static final StartupOptions CDC_STARTUP_OPTIONS = StartupOptions.latest();

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        MySqlSource<Map<String, Object>> mySqlSource = MySqlSource.<Map<String, Object>>builder()
                .hostname(CDC_HOSTNAME)
                .port(CDC_PORT)
                .username(CDC_USERNAME)
                .password(CDC_PASSWORD)
                .databaseList(CDC_DATABASE)
                .tableList(CDC_DATABASE + "." + CDC_TABLE)
                .serverId(CDC_SERVER_ID)
                .serverTimeZone(CDC_TIMEZONE)
                .startupOptions(CDC_STARTUP_OPTIONS)
                .deserializer(new MapDebeziumDeserializationSchema())
                .includeSchemaChanges(true)
                .build();
        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "CdcSource")
                .map(JsonUtils::toString)
                .returns(String.class)
                .print()
                .setParallelism(1);
        env.execute("CdcTestJob");
    }
}
