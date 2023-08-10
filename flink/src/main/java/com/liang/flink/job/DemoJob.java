package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.LocalConfigFile;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.high.level.api.StreamFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Slf4j
@LocalConfigFile("demo.yml")
public class DemoJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        StreamFactory.create(env)
                .map(new MapFunction<SingleCanalBinlog, String>() {
                    @Override
                    public String map(SingleCanalBinlog value) throws Exception {
                        return JsonUtils.toString(value);
                    }
                }).print().setParallelism(1);
        env.execute("DemoJob");
    }
}
