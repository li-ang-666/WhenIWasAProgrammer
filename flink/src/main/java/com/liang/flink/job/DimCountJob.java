package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.dto.HbaseOneRow;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.basic.StreamEnvironmentFactory;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.high.level.api.StreamFactory;
import com.liang.flink.service.data.update.DataUpdateContext;
import com.liang.flink.service.data.update.DataUpdateService;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

@Slf4j
public class DimCountJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamEnvironmentFactory.create(args);
        DataStream<SingleCanalBinlog> sourceStream = StreamFactory.create(env);
        Config config = ConfigUtils.getConfig();
        sourceStream.addSink()
    }

    @Slf4j
    private final static class DimCountSink extends RichSinkFunction<SingleCanalBinlog> {
        private final Config config;
        private DataUpdateService<HbaseOneRow> service;

        public DimCountSink(Config config) {
            this.config = config;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            ConfigUtils.setConfig(config);
            DataUpdateContext<HbaseOneRow> context = new DataUpdateContext<HbaseOneRow>
                    ("com.liang.flink.project.dim.count.impl")
                    .addClass("EntityBeneficiaryDetails")
                    .addClass("EntityControllerDetails");
            service = new DataUpdateService<>(context);
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) throws Exception {
            service.invoke(singleCanalBinlog)
        }
    }
}
