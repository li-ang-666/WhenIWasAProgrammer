package com.liang.flink.basic;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

public class StreamEnvironmentFactory {
    private StreamEnvironmentFactory() {
    }

    public static StreamExecutionEnvironment createStreamEnvironment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取checkpoint管理者
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        //每5分钟开启一次
        checkpointConfig.setCheckpointInterval(1000 * 60 * 5);
        //模式是Exactly-Once
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //每次checkpoint超时是30分钟
        checkpointConfig.setCheckpointTimeout(1000 * 60 * 30);
        //可以容忍的连续checkpoint次数,次数超过后任务自动停止
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        //同时运行的checkpoint数量
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        //两次checkpoint之间最少间隔时间
        checkpointConfig.setMinPauseBetweenCheckpoints(1000 * 60 * 5);
        //程序停止时保留checkpoint
        checkpointConfig.enableExternalizedCheckpoints(RETAIN_ON_CANCELLATION);
        //开启非对齐的checkpoint(可跳跃的barrier)
        checkpointConfig.enableUnalignedCheckpoints();
        checkpointConfig.setForceUnalignedCheckpoints(true);
        return env;
    }
}
