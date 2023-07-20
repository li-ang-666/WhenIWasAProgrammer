package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.dto.HbaseSchema;
import com.liang.common.service.database.template.HbaseTemplate;
import com.liang.common.service.filesystem.ObsWriter;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.LocalConfigFile;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

@LocalConfigFile("demo.yml")
public class DemoJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        env.addSource(new DemoSource(config))
                .addSink(new DemoSink(config));
        env.execute("DemoJob");
    }

    @Slf4j
    @RequiredArgsConstructor
    private static class DemoSource extends RichSourceFunction<String> {
        private final Config config;
        private HbaseTemplate hbaseTemplate;

        @Override
        public void open(Configuration parameters) throws Exception {
            ConfigUtils.setConfig(config);
            hbaseTemplate = new HbaseTemplate("hbaseSink");
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            hbaseTemplate.scan(HbaseSchema.HUMAN_ALL_COUNT,
                    hbaseOneRow -> ctx.collect(JsonUtils.toString(hbaseOneRow)));
        }

        @Override
        public void cancel() {
        }
    }

    @Slf4j
    @RequiredArgsConstructor
    private static class DemoSink extends RichSinkFunction<String> {
        private final Config config;
        private final ObsWriter obsWriter;

        @Override
        public void open(Configuration parameters) throws Exception {
            ConfigUtils.setConfig(config);
        }

        @Override
        public void invoke(String input, Context context) throws Exception {
            Tuple2<byte[], String> value = input.getValue();
            log.info("binlog: {}", value.f0);
        }
    }
}
