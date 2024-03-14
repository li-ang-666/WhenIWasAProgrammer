package com.liang.flink.job;


import cn.hutool.http.HttpUtil;
import com.liang.common.dto.Config;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.StreamFactory;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.service.LocalConfigFile;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@LocalConfigFile("bid.yml")
public class BidJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        StreamFactory.create(env)
                .keyBy(e -> e.getColumnMap().get("uuid"))
                .addSink(new BidSink(config))
                .setParallelism(config.getFlinkConfig().getOtherParallel())
                .name("BidSink")
                .uid("BidSink");
        env.execute("BidJob");
    }

    @RequiredArgsConstructor
    private static final class BidSink extends RichSinkFunction<SingleCanalBinlog> {
        private static final int TIMEOUT = 1000 * 60;
        private static final String URL = "http://10.99.199.173:10040/linking_yuqing_rank";
        private final Config config;

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) {
            Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
            String uuid = String.valueOf(columnMap.get("uuid"));
            String content = String.valueOf(columnMap.get("content"));
            doPost(content, uuid);
        }

        private void doPost(String content, String uuid) {
            Map<String, Object> paramMap = new HashMap<>();
            paramMap.put("text", content);
            paramMap.put("bid_uuid", uuid);
            String result = HttpUtil.post(URL, paramMap, TIMEOUT);
            log.info("post result: {}", result);
        }
    }
}
