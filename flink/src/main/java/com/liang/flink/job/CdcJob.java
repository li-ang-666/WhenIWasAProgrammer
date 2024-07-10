package com.liang.flink.job;

import com.alibaba.otter.canal.protocol.FlatMessage;
import com.liang.common.util.JsonUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.cdc.CanalDebeziumDeserializationSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Slf4j
public class CdcJob {
    // cdc
    private static final String CDC_HOSTNAME = "9349c027b3b4414aa5f9019cd218e7a3in01.internal.cn-north-4.mysql.rds.myhuaweicloud.com";
    private static final String CDC_DATABASE = "test";
    private static final String CDC_TABLE = "test";
    private static final int CDC_PORT = 3306;
    private static final String CDC_USERNAME = "canal_d";
    private static final String CDC_PASSWORD = "Canal@Dduan";
    private static final String CDC_SERVER_ID = "6000-6100";
    private static final String CDC_TIMEZONE = "Asia/Shanghai";
    private static final StartupOptions CDC_STARTUP_OPTIONS = StartupOptions.latest();
    // kafka
    private static final String KAFKA_BOOTSTRAP_SERVER = "10.99.202.90:9092,10.99.206.80:9092,10.99.199.2:9092";
    private static final String KAFKA_TOPIC = "abc";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        MySqlSource<FlatMessage> mySqlSource = MySqlSource.<FlatMessage>builder()
                .hostname(CDC_HOSTNAME)
                .port(CDC_PORT)
                .username(CDC_USERNAME)
                .password(CDC_PASSWORD)
                .databaseList(CDC_DATABASE)
                .tableList(CDC_DATABASE + "." + CDC_TABLE)
                .serverId(CDC_SERVER_ID)
                .serverTimeZone(CDC_TIMEZONE)
                .startupOptions(CDC_STARTUP_OPTIONS)
                .deserializer(new CanalDebeziumDeserializationSchema())
                .build();
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP_SERVER)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(KAFKA_TOPIC)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "CdcSource")
                .name("CdcSource")
                .uid("CdcSource")
                .setParallelism(1)
                .rebalance()
                .map(JsonUtils::toString)
                .name("FlatMessageMapper")
                .uid("FlatMessageMapper")
                .setParallelism(1)
                .returns(String.class)
                .sinkTo(kafkaSink)
                .name("KafkaSink")
                .uid("KafkaSink")
                .setParallelism(1);
        env.execute("CdcJob");
    }
}
