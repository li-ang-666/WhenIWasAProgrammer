package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.LocalConfigFile;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.high.level.api.StreamFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Slf4j
@LocalConfigFile("demo.yml")
public class DemoJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        DataStream<SingleCanalBinlog> stream = StreamFactory.create(env);
        stream.filter(new FilterFunction<SingleCanalBinlog>() {
            @Override
            public boolean filter(SingleCanalBinlog value) throws Exception {
                if (value.getColumnMap().get("company_id_invested").equals("923037552")) {
                    log.error("----------------{}", value);
                }
                return value.getColumnMap().get("company_id_invested").equals("923037552");
            }
        }).setParallelism(20).print().setParallelism(1);
        env.execute("DemoJob");
    }
}
