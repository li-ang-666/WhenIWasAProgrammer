package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.LocalConfigFile;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.high.level.api.StreamFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.Map;
import java.util.Objects;

@Slf4j
@LocalConfigFile("demo.yml")
public class DemoJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        StreamFactory.create(env)
                .addSink(new SinkFunction<SingleCanalBinlog>() {
                    @Override
                    public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) throws Exception {
                        System.out.println(JsonUtils.toString(singleCanalBinlog));
                        Map<String, Object> beforeColumnMap = singleCanalBinlog.getBeforeColumnMap();
                        Map<String, Object> afterColumnMap = singleCanalBinlog.getAfterColumnMap();
                        System.out.println("name: " + Objects.deepEquals(beforeColumnMap.get("name"), afterColumnMap.get("name")));
                        System.out.println("deleted: " + Objects.deepEquals(beforeColumnMap.get("deleted"), afterColumnMap.get("deleted")));
                    }
                });
        env.execute("DemoJob");
    }
}
