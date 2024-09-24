package com.liang.flink.basic.repair;

import cn.hutool.core.util.StrUtil;
import com.liang.common.dto.Config;
import com.liang.common.service.database.template.RedisTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.List;

/*
 * https://nightlies.apache.org/flink/flink-docs-release-1.17/zh/docs/dev/datastream/fault-tolerance/checkpointing
 * 部分任务结束后的 Checkpoint
 */
@Slf4j
@RequiredArgsConstructor
public class RepairSource extends RichSourceFunction<List<RepairSplit>> {
    private final Config config;
    private final String repairKey;
    private RedisTemplate redisTemplate;

    @Override
    public void open(Configuration parameters) throws Exception {
        ConfigUtils.setConfig(config);
        redisTemplate = new RedisTemplate("metadata");
    }

    @Override
    public void run(SourceContext<List<RepairSplit>> ctx) {
        // 生成
        List<RepairSplit> repairSplits = new RepairGenerator().generate();
        // 打印
        reportAndLog(StrUtil.repeat("=", 50));
        for (RepairSplit repairSplit : repairSplits) reportAndLog(JsonUtils.toString(repairSplit));
        reportAndLog(StrUtil.repeat("=", 50));
        // 发送下游
        ctx.collect(repairSplits);
    }

    @Override
    public void cancel() {
    }

    private void reportAndLog(String logs) {
        logs = String.format("[RepairSource] %s", logs);
        redisTemplate.rPush(repairKey, logs);
        log.info("{}", logs);
    }
}
