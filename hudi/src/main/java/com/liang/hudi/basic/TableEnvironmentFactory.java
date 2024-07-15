package com.liang.hudi.basic;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.concurrent.TimeUnit;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

@Slf4j
@UtilityClass
public class TableEnvironmentFactory {
    private final static long CHECKPOINT_INTERVAL = 1000 * 60 * 10;
    private final static long CHECKPOINT_TIMEOUT = 1000 * 60 * 30;

    public static StreamTableEnvironment create(boolean isBatchMode) {
        StreamExecutionEnvironment env = initEnv();
        configEnv(env);
        if (isBatchMode) {
            env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        }
        return StreamTableEnvironment.create(env);
    }

    private static StreamExecutionEnvironment initEnv() {
        StreamExecutionEnvironment tempEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        return tempEnv instanceof LocalStreamEnvironment ? initLocalEnv() : initClusterEnv();
    }

    private static StreamExecutionEnvironment initLocalEnv() {
        Configuration configuration = new Configuration();
        configuration.setString("rest.bind-port", "54321");
        configuration.setString("state.checkpoints.dir", "file:///Users/liang/Desktop/flink-checkpoints");
        //configuration.setString("taskmanager.memory.network.min", "1g");
        //configuration.setString("taskmanager.memory.network.max", "1g");
        //configuration.setString("execution.savepoint.path", "file://" + "/Users/liang/Desktop/flink-checkpoints/53b0ef2c94cda86ea614605757352069/chk-2");
        return StreamExecutionEnvironment.getExecutionEnvironment(configuration);
    }

    private static StreamExecutionEnvironment initClusterEnv() {
        return StreamExecutionEnvironment.getExecutionEnvironment();
    }

    private static void configEnv(StreamExecutionEnvironment env) {
        // 统一checkpoint管理
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // 运行周期
        checkpointConfig.setCheckpointInterval(CHECKPOINT_INTERVAL);
        // 两次checkpoint之间最少间隔时间
        checkpointConfig.setMinPauseBetweenCheckpoints(CHECKPOINT_INTERVAL);
        // 模式是Exactly-Once
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 超时
        checkpointConfig.setCheckpointTimeout(CHECKPOINT_TIMEOUT);
        // 可以容忍的连续checkpoint次数,次数超过后任务自动停止
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(1024);
        // 同时运行的checkpoint数量
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        // 程序停止时保留checkpoint
        checkpointConfig.setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);
        // 开启非对齐的checkpoint(可跳跃的barrier)
        checkpointConfig.enableUnalignedCheckpoints();
        // hudi bucket索引 需要开启这个
        checkpointConfig.setForceUnalignedCheckpoints(true);
        if (env instanceof LocalStreamEnvironment) {
            // 运行周期
            checkpointConfig.setCheckpointInterval(TimeUnit.SECONDS.toMillis(5));
            // 两次checkpoint之间最少间隔时间
            checkpointConfig.setMinPauseBetweenCheckpoints(TimeUnit.SECONDS.toMillis(5));
        }
    }
}
