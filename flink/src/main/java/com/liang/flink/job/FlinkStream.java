package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.YamlUtils;
import com.liang.flink.basic.FlinkKafkaSourceFactory;
import com.liang.flink.basic.StreamEnvironmentFactory;
import com.liang.flink.dto.BatchCanalBinlog;
import com.liang.flink.dto.KafkaRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.io.InputStream;

@Slf4j
public class FlinkStream {
    public static void main(String[] args) throws Exception {
        init();
        StreamExecutionEnvironment streamEnvironment = StreamEnvironmentFactory.createStreamEnvironment();
        FlinkKafkaConsumer<KafkaRecord<BatchCanalBinlog>> kafkaSource = FlinkKafkaSourceFactory.createFlinkKafkaStream(BatchCanalBinlog::new);

        streamEnvironment
                .addSource(kafkaSource).setParallelism(1)
                .addSink(new SinkFunction<KafkaRecord<BatchCanalBinlog>>() {
                    @Override
                    public void invoke(KafkaRecord<BatchCanalBinlog> value, Context context) throws Exception {
                        System.out.println(value);
                    }
                }).setParallelism(1);
        streamEnvironment.execute();
    }

    private static void init() {
        InputStream resourceStream = FlinkStream.class.getClassLoader().getResourceAsStream("config.yml");
        Config config = YamlUtils.parse(resourceStream, Config.class);
        ConfigUtils.setConfig(config);
    }
}



