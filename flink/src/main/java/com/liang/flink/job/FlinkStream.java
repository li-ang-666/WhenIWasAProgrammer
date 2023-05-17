package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.YamlUtils;
import com.liang.flink.basic.FlinkKafkaConsumerFactory;
import com.liang.flink.basic.StreamEnvironmentFactory;
import com.liang.flink.dto.KafkaData;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.InputStream;

public class FlinkStream {
    public static void main(String[] args) throws Exception {
        init();
        StreamExecutionEnvironment streamEnvironment = StreamEnvironmentFactory.createStreamEnvironment();
        DataStreamSource<KafkaData> source = streamEnvironment.addSource(FlinkKafkaConsumerFactory.createFlinkKafkaStream()).setParallelism(1);
        source.print().setParallelism(1);
        streamEnvironment.execute();
    }

    private static void init() {
        InputStream resourceStream = FlinkStream.class.getClassLoader().getResourceAsStream("config.yml");
        Config config = YamlUtils.parse(resourceStream, Config.class);
        ConfigUtils.setConfig(config);
    }
}



