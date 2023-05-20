package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.YamlUtils;
import com.liang.flink.basic.FlinkKafkaSourceFactory;
import com.liang.flink.basic.StreamEnvironmentFactory;
import com.liang.flink.dto.BatchCanalBinlog;
import com.liang.flink.dto.KafkaRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.io.InputStream;

@Slf4j
public class FlinkStream {
    public static void main(String[] args) throws Exception {
        init();
        StreamExecutionEnvironment streamEnvironment = StreamEnvironmentFactory.createStreamEnvironment();
        streamEnvironment.setParallelism(1);
        FlinkKafkaConsumer<KafkaRecord<BatchCanalBinlog>> kafkaSource = FlinkKafkaSourceFactory.createFlinkKafkaStream(BatchCanalBinlog::new);

        streamEnvironment
                .addSource(kafkaSource)
                .keyBy(new KeySelector<KafkaRecord<BatchCanalBinlog>, KafkaRecord<BatchCanalBinlog>>() {
                    @Override
                    public KafkaRecord<BatchCanalBinlog> getKey(KafkaRecord<BatchCanalBinlog> value) throws Exception {
                        return value;
                    }
                })
                .addSink(new SkFunction());

        streamEnvironment.execute();
    }

    private static void init() {
        InputStream resourceStream = FlinkStream.class.getClassLoader().getResourceAsStream("config.yml");
        Config config = YamlUtils.parse(resourceStream, Config.class);
        ConfigUtils.setConfig(config);
    }
}

@Slf4j
class SkFunction extends RichSinkFunction<KafkaRecord<BatchCanalBinlog>> {
    MapState<String, Object> mapState;
    int i;

    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<String, Object> descriptor = new MapStateDescriptor<>("name", String.class, Object.class);
        mapState = getRuntimeContext().getMapState(descriptor);
    }

    @Override
    public void invoke(KafkaRecord<BatchCanalBinlog> value, Context context) throws Exception {
        mapState.put(String.valueOf(i++), value);
        log.info("value: {}", value);
    }
}


