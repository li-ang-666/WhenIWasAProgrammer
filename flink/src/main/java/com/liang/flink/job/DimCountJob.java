package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.basic.StreamEnvironmentFactory;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.high.level.api.StreamFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class DimCountJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamEnvironmentFactory.create(args);
        DataStream<SingleCanalBinlog> sourceStream = StreamFactory.create(env);
        Config config = ConfigUtils.getConfig();
        sourceStream.addSink()
    }

    private final static class DimCountSink extends RichSinkFunction<SingleCanalBinlog> {
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
        }

        @Override
        public void invoke(SingleCanalBinlog value, Context context) throws Exception {
            super.invoke(value, context);
        }
    }
}
