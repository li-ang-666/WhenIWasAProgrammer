package com.liang.flink.basic;

import com.liang.common.dto.Config;
import com.liang.common.util.ConfigUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

@Slf4j
public class StreamEnvironmentFactory {
    private final static long CHECKPOINT_INTERVAL = 1000 * 60 * 3;
    private final static long CHECKPOINT_TIMEOUT = 1000 * 60 * 10;

    private StreamEnvironmentFactory() {
    }

    public static StreamExecutionEnvironment create(String[] args) throws Exception {
        initConfig(args);
        return initEnv();
    }

    private static void initConfig(String[] args) throws Exception {
        Config config = ConfigUtils.initConfig(args);
        ConfigUtils.setConfig(config);
    }

    private static StreamExecutionEnvironment initEnv() {
        StreamExecutionEnvironment env;
        if (StreamExecutionEnvironment.getExecutionEnvironment() instanceof LocalStreamEnvironment) {
            env = initLocalEnv();
        } else {
            env = initClusterEnv();
        }
        //统一checkpoint管理
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        //每5分钟开启一次
        checkpointConfig.setCheckpointInterval(CHECKPOINT_INTERVAL);
        //两次checkpoint之间最少间隔时间
        checkpointConfig.setMinPauseBetweenCheckpoints(CHECKPOINT_INTERVAL);
        //模式是Exactly-Once
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //每次checkpoint超时是30分钟
        checkpointConfig.setCheckpointTimeout(CHECKPOINT_TIMEOUT);
        //可以容忍的连续checkpoint次数,次数超过后任务自动停止
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        //同时运行的checkpoint数量
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        //程序停止时保留checkpoint
        checkpointConfig.setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);
        //开启非对齐的checkpoint(可跳跃的barrier)
        checkpointConfig.enableUnalignedCheckpoints();
        checkpointConfig.setForceUnalignedCheckpoints(true);
        return env;
    }

    private static StreamExecutionEnvironment initLocalEnv() {
        Configuration configuration = new Configuration();

        //本地测试参数
        configuration.setString("rest.bind-port", "54321");

        //本地checkpoint调试
        configuration.setString("state.checkpoints.dir", "file:///Users/liang/Desktop/flink-checkpoints/");
        //configuration.setString("execution.savepoint.path", "file://" + "/Users/liang/Desktop/flink-checkpoints/53b0ef2c94cda86ea614605757352069/chk-2");
        return StreamExecutionEnvironment.getExecutionEnvironment(configuration);
    }

    private static StreamExecutionEnvironment initClusterEnv() {
        Configuration configuration = new Configuration();
        configuration.setString("yarn.application.name", "AAAAAAAAAAAAAAAAAA");
        configuration.setString("taskmanager.numberOfTaskSlots", "2");
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        executionEnvironment.configure(configuration);
        return executionEnvironment;
    }
}
